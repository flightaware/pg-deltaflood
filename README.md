BUILDING
--------

```
cd postgres
autoreconf; ./configure suitable-options
cd contrib/tsv_streamer
gmake; sudo gmake install
```

USING
-----

```
pg_recvlogical --create-slot --slot streamer --plugin=tsv_streamer
pg_recvlogical --start --slot streamer --file=- -o opt=value...
```

Options:

 include-xids=bool default true
  include _xid pseudo-column
 include-oids=bool default true
  include _oid pseudo_column
 include-lsn=bool default true
  include _lsn pseudo_column
 full=name=bool default false
  use fully qualified table name with schema
 skip-nulls=bool default true
  Don't even output columns with null values
 escape_chars=bool default true
  escape tab as \t, newline as \n, other control characters as \NNN in octal
 null=null_string default NULL
  if skip-nulls=false, then use this as the null string
 separator=tab default \t
  separate columns with this string
 tables=table,table,table default all tables
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
