DELTAFLOOD
==========

This is a PostgreSQL output plugin that provides a way to stream deltas to a database in an easily _and rapidly_ parsed format for
mirroring those changes to an external application. It is based on the "test_decoding" plugin shipped with the PostgreSQL
distribution.

The output format consists of a stream of tab-separated lines, each line containing the changes to the database, along with
some extra pseudo-columns:

* _table table_name
* _xid (optional)
* _oid (optional)
* _lsn (optional)
* _action [insert|replace|update|delete]

The "delete" actions are only output for tables with a primary key that can be used to uniquely identify deleted rows.

Updates will be preceded by a replace line with the old primary key when appropriate. The reader can either
treat "update" as "insert" and "replace" as "delete", or generate an update operation with an appropriate "where" clause if it's preceded by a change.

You really need a primary key or OIDs (oh, hey, that's a primary key) if you want to usefully use this for replication. :)

SETUP
-----

deltaflood now uses PGXN so you only need to follow the following instructions for PostgreSQL <= 8.1 and why would you do that?

This depends on a full copy of the postgres build tree being available. The easiest way to do this is to "git clone" the
Postgres repo and this repo under the same common parent directory. If the postgres build tree is somewhere other than
../postgres you will need to modify "top_builddir" in Makefile to point to it.

```
git clone git@github.com:flightaware/pg-deltaflood.git
git clone git@github.com:postgres/postgres.git
```

Or

```
git clone https://github.com/flightaware/pg-deltaflood.git
git clone https://github.com/postgres/postgres.git
```


BUILDING
--------

With current versions of Postgres, if you have it properly set up so `pg_config --pgxs` returns something like `/usr/local/lib/postgresql/pgxs/src/makefiles/pgxs.mk`, just "gmake; gmake install"

With PostgreSQL <= 8.1:

After setting up the build trees for postgres and deltaflood next to each other:

```
cd ../postgres
autoreconf; ./configure $suitable-options
gmake
cd ../pg-deltaflood
gmake NO_PGXS=1; sudo gmake NO_PGXS=1 install
```

USING
-----

```
pg_recvlogical --create-slot --slot streamer --plugin=deltaflood
pg_recvlogical --start --slot streamer --file=- -o opt=value...
```

Options:

* include-xids=bool default true

    include _xid (Transaction ID) pseudo-column
    
* include-oids=bool default true

    include _oid (object ID) pseudo_column
    
* include-lsn=bool default false

    include _lsn (Logical Serial Number) pseudo_column
    
* full-name=bool default false

    use fully qualified table name with schema
    
* skip-nulls=bool default true

    Don't even output columns with null values
    
* escape_chars=bool default true

    escape tab as \t, newline as \n, other control characters as \NNN in octal
    
* convert-bool=bool default true

    convert boolean values to "1" or "0" regardless of original representation

* null=null_string default NULL

    if skip-nulls=false, then use this as the null string
    
* separator=tab default \t

    separate columns with this string
    
* tables=table,table,table default all tables

    Only output rows for listed tables

Output example
--------------

```
$ pg_recvlogical --start --slot streamer --file=- -o tables=bar,baz -o include-oids
_table	bar	_xid	579	_action	insert	a	cat	b	meow
_table	bar	_xid	581	_action	insert	a	cat	b	meow
_table	baz	_xid	582	_action	insert	_oid	16405	a	rocket	b	science
_table	baz	_xid	584	_action	insert	_oid	16406	a	rocket	b	science
^C
$ pg_recvlogical --start --slot streamer --file=- -o include-lsn 
_table	zzz	_xid	7619	_lsn	0/177B148	_action	insert	a	able	b	a
_table	zzz	_xid	7620	_lsn	0/177B148	_action	delete	a	able

```
