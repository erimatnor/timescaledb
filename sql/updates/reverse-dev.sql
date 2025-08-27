DROP FUNCTION IF EXISTS @extschema@.generate_uuidv7();
DROP FUNCTION IF EXISTS @extschema@.to_uuidv7(timestamptz);
DROP FUNCTION IF EXISTS @extschema@.to_uuidv7_boundary(timestamptz);
DROP FUNCTION IF EXISTS @extschema@.uuid_timestamp(uuid);
DROP FUNCTION IF EXISTS @extschema@.uuid_timestamp_micros(uuid);
DROP FUNCTION IF EXISTS @extschema@.uuid_version(uuid);

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 7 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_UUID';

