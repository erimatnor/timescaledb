CREATE FUNCTION timescaledb_fdw_handler()
RETURNS fdw_handler
AS '@MODULE_PATHNAME@', 'timescaledb_fdw_handler'
LANGUAGE C STRICT;


CREATE FOREIGN DATA WRAPPER timescaledb_fdw
  HANDLER timescaledb_fdw_handler;
