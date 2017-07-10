# contrib/deltaflood/Makefile

MODULES = deltaflood
PGFILEDESC = "deltaflood - stream specific tables into TSV format"

# Note: because we don't tell the Makefile there are any regression tests,
# we have to clean those result files explicitly
EXTRA_CLEAN = $(pg_regress_clean_files)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = ../pg-deltaflood
top_builddir = ../postgres
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Disabled because these tests require "wal_level=logical", which
# typical installcheck users do not have (e.g. buildfarm clients).
installcheck:;

# But it can nonetheless be very helpful to run tests on preexisting
# installation, allow to do so, but only if requested explicitly.
installcheck-force: regresscheck-install-force isolationcheck-install-force

# Disabled because there's no tests for deltaflood
check:;
#check: regresscheck isolationcheck

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

submake-deltaflood:
	$(MAKE) -C $(top_builddir)/../pg-deltaflood

REGRESSCHECKS=ddl xact rewrite toast permissions decoding_in_xact \
	decoding_into_rel binary prepared replorigin time messages \
	spill slot

regresscheck: | submake-regress submake-deltaflood temp-install
	$(pg_regress_check) \
	    --temp-config $(top_srcdir)/../pg-deltaflood/logical.conf \
	    $(REGRESSCHECKS)

regresscheck-install-force: | submake-regress submake-deltaflood temp-install
	$(pg_regress_installcheck) \
	    $(REGRESSCHECKS)

ISOLATIONCHECKS=mxact delayed_startup ondisk_startup concurrent_ddl_dml

isolationcheck: | submake-isolation submake-deltaflood temp-install
	$(pg_isolation_regress_check) \
	    --temp-config $(top_srcdir)/../pg-deltaflood/logical.conf \
	    $(ISOLATIONCHECKS)

isolationcheck-install-force: all | submake-isolation submake-deltaflood temp-install
	$(pg_isolation_regress_installcheck) \
	    $(ISOLATIONCHECKS)

.PHONY: submake-deltaflood submake-regress check \
	regresscheck regresscheck-install-force \
	isolationcheck isolationcheck-install-force

temp-install: EXTRA_INSTALL=../pg-deltaflood
