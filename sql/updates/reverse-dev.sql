DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Revert support for concurrent merge chunks()
DROP PROCEDURE IF EXISTS _timescaledb_functions.chunk_rewrite_cleanup();
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks_concurrently(REGCLASS[]);
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS, BOOLEAN);
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_rewrite;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_rewrite;

