DROP FUNCTION _timescaledb_internal.ping_data_node(NAME);

CREATE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME, timeout INTERVAL = NULL) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

DROP FUNCTION _timescaledb_internal.dimension_is_finite;
DROP FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql;
