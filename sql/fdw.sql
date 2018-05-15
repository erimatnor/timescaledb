CREATE FUNCTION timescaledb_fdw_handler()
RETURNS fdw_handler
AS '@MODULE_PATHNAME@', 'timescaledb_fdw_handler'
LANGUAGE C STRICT;


CREATE FUNCTION timescaledb_fdw_validator(text[], oid)
RETURNS void
AS '@MODULE_PATHNAME@', 'timescaledb_fdw_handler'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER timescaledb
  HANDLER timescaledb_fdw_handler
  VALIDATOR timescaledb_fdw_validator;
