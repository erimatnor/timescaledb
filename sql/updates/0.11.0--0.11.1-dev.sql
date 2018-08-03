DO language plpgsql $$
BEGIN
  RAISE WARNING '%',
 E'\nStarting in v0.12.0, TimescaleDB collects anonymous reports to better understand and assist our
users. For more information and how to disable, please see our docs https://docs.timescaledb.com/using-timescaledb/telemetry.\n';
END;
$$;
CREATE FUNCTION _timescaledb_internal.generate_uuid_external() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'generate_uuid_external' LANGUAGE C VOLATILE STRICT;
INSERT INTO _timescaledb_catalog.installation_metadata SELECT 'uuid', _timescaledb_internal.generate_uuid_external();
INSERT INTO _timescaledb_catalog.installation_metadata SELECT 'install_timestamp', GetCurrentTimestamp();
DROP FUNCTION _timescaledb_internal.generate_uuid_external();
