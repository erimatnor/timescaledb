DROP FUNCTION IF EXISTS _timescaledb_functions.generate_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_v7_from_timestamptz;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_v7_from_timestamptz_zeroed;
DROP FUNCTION IF EXISTS _timescaledb_functions.timestamptz_from_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.timestamptz_from_uuid_v7_with_microseconds;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_version;

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 7 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_UUID';

