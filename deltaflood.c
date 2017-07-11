/*-------------------------------------------------------------------------
 *
 * deltaflood.c
 *		  stream specific tables as TSV files in name-value format
 *		  Based on contributed example logical decoding output plugin
 *
 * Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/deltaflood/deltaflood.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

// TODO: replace this with the Postgres ListCell and List structures.
typedef struct _n
{
	struct _n *next;
	char *name;
} namelist;

typedef struct
{
	MemoryContext context;
	bool		include_xids;
	bool		include_oids;
	bool		include_lsn;
	bool		full_name;
	bool		skip_nulls;
	bool		only_local;
	namelist	*table_list;
	char		*null_string;
	char		*sep_string;
	bool		escape_chars;
} DeltaFloodData;

static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);
static bool pg_decode_filter(LogicalDecodingContext *ctx,
				 RepOriginId origin_id);
static void pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr message_lsn,
				  bool transactional, const char *prefix,
				  Size sz, const char *message);

static namelist *nladdname(namelist *list, char *name);
static int nlcheckname(namelist *node, char *name);

static namelist *nladdname(namelist *list, char *name)
{
	namelist *node = palloc(sizeof *node);
	node->next = list;
	node->name = pstrdup(name);
	return node;
}

static int nlcheckname(namelist *node, char *name)
{
	while(node) {
		if(node->name && strcmp(node->name, name) == 0)
			return 1;
		node = node->next;
	}
	return 0;
}

static void nlfree(namelist *node)
{
	while(node) {
		namelist *next = node->next;
		if(node->name) pfree(node->name);
		pfree(node);
		node = next;
	}
}

void
_PG_init(void)
{
	/* other plugins can perform things here */
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->filter_by_origin_cb = pg_decode_filter;
	cb->shutdown_cb = pg_decode_shutdown;
	cb->message_cb = pg_decode_message;
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	ListCell   *option;
	DeltaFloodData *data;

	data = palloc0(sizeof(DeltaFloodData));
	data->context = AllocSetContextCreate(ctx->context,
										  "text conversion context",
										  ALLOCSET_DEFAULT_SIZES);
	data->include_xids = true;
	data->include_oids = true;
	data->include_lsn = false;
	data->full_name = false;
	data->skip_nulls = true;
	data->only_local = false;
	data->table_list = NULL;
	data->null_string = NULL;
	data->sep_string = NULL;
	data->escape_chars = true;

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-xids") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_xids = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_xids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "skip-nulls") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->skip_nulls = true;
			else if (!parse_bool(strVal(elem->arg), &data->skip_nulls))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "escape-chars") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->escape_chars = true;
			else if (!parse_bool(strVal(elem->arg), &data->escape_chars))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "full-name") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->full_name = true;
			else if (!parse_bool(strVal(elem->arg), &data->full_name))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-oids") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_oids = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_oids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-lsn") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_lsn = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_lsn))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "only-local") == 0)
		{

			if (elem->arg == NULL)
				data->only_local = true;
			else if (!parse_bool(strVal(elem->arg), &data->only_local))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "separator") == 0)
		{
			if(elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("missing value for parameter \"%s\"",
								elem->defname)));
			else
				data->sep_string = pstrdup(strVal(elem->arg));
		}
		else if (strcmp(elem->defname, "null") == 0)
		{
			if(elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("missing value for parameter \"%s\"",
								elem->defname)));
			else
				data->null_string = pstrdup(strVal(elem->arg));
		}
		else if (strcmp(elem->defname, "tables") == 0)
		{
			if(elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("missing value for parameter \"%s\"",
								elem->defname)));
			else {
				namelist *table_list = NULL;
				char *next_table = strVal(elem->arg);
				while(next_table) {
					char *this_table = next_table;
					next_table = strchr(next_table, ',');
					if(next_table) *next_table = 0;
					table_list = nladdname(table_list, this_table);
					if(next_table) *next_table++ = ',';
				}
				data->table_list = table_list;
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

/* cleanup this plugin's resources */
static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
	DeltaFloodData *data = ctx->output_plugin_private;

	/* clean up configuration resources */
	nlfree(data->table_list);
	data->table_list = NULL;
	if(data->null_string) {
		pfree(data->null_string);
		data->null_string = NULL;
	}
	if(data->sep_string) {
		pfree(data->sep_string);
		data->sep_string = NULL;
	}

	/* cleanup our own resources via memory context reset */
	MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	// Ignore BEGIN.
}

/* COMMIT callback */
static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	// Ignore COMMIT.
}

static bool
pg_decode_filter(LogicalDecodingContext *ctx,
				 RepOriginId origin_id)
{
	DeltaFloodData *data = ctx->output_plugin_private;

	if (data->only_local && origin_id != InvalidRepOriginId)
		return true;
	return false;
}

/*
 * Append literal `outputstr' already represented as string into stringbuf `s'.
 *
 * Replace tabs with "\t", newlines with "\n", other binary with "\xxx".
 */
static void
appendStringEscaped(StringInfo s, char *outputstr)
{
	const char *valptr;

	for (valptr = outputstr; *valptr; valptr++)
	{
		char		ch = *valptr;
		if(ch == '\t')
			appendStringInfoString(s, "\\t");
		else if(ch == '\n')
			appendStringInfoString(s, "\\n");
		else if(ch < ' ')
			appendStringInfo(s, "\\%03o", ch);
		else
			appendStringInfoChar(s, ch);
	}
}

/* print the tuple 'tuple' into the StringInfo s */
static void
appendTupleAsTSV(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, DeltaFloodData *data)
{
	int			natt;
	Oid			oid;
	char *sep_string = data->sep_string ? data->sep_string : "\t";

	/* print oid of tuple, it's not included in the TupleDesc */
	if (data->include_oids && (oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid)
	{
		appendStringInfo(s, "%s_oid%s%u", sep_string, sep_string, oid);
	}

	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		char		*stringval;	/* Toasted or not, it gets converted to this. */
		bool		isnull;		/* column is null? */

		attr = tupdesc->attrs[natt];

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull) {
			if(data->skip_nulls)
				continue;

			stringval = data->null_string ? data->null_string : "NULL";
		} else {
			/* query output function */
			getTypeOutputInfo(typid, &typoutput, &typisvarlena);

			if (typisvarlena) {
				/* Skip weird data */
				if (VARATT_IS_EXTERNAL_ONDISK(origval))
					continue;

				stringval = OidOutputFunctionCall(typoutput, PointerGetDatum(PG_DETOAST_DATUM(origval)));
			} else {
				stringval = OidOutputFunctionCall(typoutput, origval);
			}
		}

		appendStringInfoString(s, sep_string);
		if(data->escape_chars)
			appendStringEscaped(s, NameStr(attr->attname));
		else
			appendStringInfoString(s, NameStr(attr->attname));

		appendStringInfoString(s, sep_string);
		if(data->escape_chars)
			appendStringEscaped(s, stringval);
		else
			appendStringInfoString(s, stringval);
	}
}

/*
 * callback for individual changed tuples
 */
static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	DeltaFloodData *data;
	Form_pg_class class_form;
	TupleDesc	tupdesc;
	MemoryContext old;
	char *table_name;
	char *sep_string;

	// May generate multiple lines
	int ntuples;
	ReorderBufferTupleBuf *tuples[2];
	char *actions[2];
	int i;

	ntuples = 0;
	switch(change->action) {
		case REORDER_BUFFER_CHANGE_INSERT:
			if(change->data.tp.newtuple) {
				tuples[ntuples] = change->data.tp.newtuple;
				actions[ntuples] = "insert";
				ntuples++;
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if(change->data.tp.oldtuple) {
				tuples[ntuples] = change->data.tp.oldtuple;
				actions[ntuples] = "replace";
				ntuples++;
			}
			if(change->data.tp.newtuple) {
				tuples[ntuples] = change->data.tp.newtuple;
				actions[ntuples] = "update";
				ntuples++;
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if(change->data.tp.oldtuple) {
				tuples[ntuples] = change->data.tp.oldtuple;
				actions[ntuples] = "delete";
				ntuples++;
			}
			break;
		default:
			// shouldn't happen
			break;
	}

	// filter empty tuples
	if (ntuples == 0)
		return;

	data = ctx->output_plugin_private;
	class_form = RelationGetForm(relation);

	// filter tables
	table_name = NameStr(class_form->relname);
	if (data->table_list && !nlcheckname(data->table_list, table_name))
		return;

	tupdesc = RelationGetDescr(relation);

	sep_string = data->sep_string ? data->sep_string : "\t";

	for(i = 0; i < ntuples; i++) {
		/* Avoid leaking memory by using and resetting our own context */
		old = MemoryContextSwitchTo(data->context);

		OutputPluginPrepareWrite(ctx, true);

		// Output _table_name\t$table_name
		appendStringInfo(ctx->out, "_table%s", sep_string);
		appendStringInfoString(ctx->out, table_name);
		if(data->full_name) {
			appendStringInfo(ctx->out, "%s_qualified_table%s", sep_string, sep_string);
			appendStringInfoString(ctx->out, quote_qualified_identifier( get_namespace_name( get_rel_namespace(RelationGetRelid(relation))), table_name));
		}

		if(data->include_xids)
			appendStringInfo(ctx->out, "%s_xid%s%d", sep_string, sep_string, txn->xid);

		if(data->include_lsn) {
			uint64 lsn = txn->restart_decoding_lsn;
			appendStringInfo(ctx->out, "%s_lsn%s%lX/%lX", sep_string, sep_string, ((lsn >> 32) & 0xFFFFFFFF), (lsn & 0xFFFFFFFF) );
		}

		// Output \t_action\t$action
		appendStringInfo(ctx->out, "%s_action%s%s", sep_string, sep_string, actions[i]);

		// Output new tuple
		appendTupleAsTSV(ctx->out, tupdesc, &tuples[i]->tuple, data);

		MemoryContextSwitchTo(old);
		MemoryContextReset(data->context);

		OutputPluginWrite(ctx, true);
	}
}

static void
pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr lsn, bool transactional,
				  const char *prefix, Size sz, const char *message)
{
	// Ignore messages
}
