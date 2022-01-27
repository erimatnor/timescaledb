/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_STATS_H
#define TIMESCALEDB_TELEMETRY_STATS_H

#include <postgres.h>

#include "utils.h"

typedef enum StatsType
{
	STATS_TYPE_BASE,
	STATS_TYPE_STORAGE,
	STATS_TYPE_HYPER
} StatsType;

typedef struct BaseStats
{
	int64 relcount;
} BaseStats;

typedef struct StorageStats
{
	BaseStats base;
	int64 relpages;
	int64 reltuples;
	RelationSize relsize;
} StorageStats;

typedef struct HyperStats
{
	StorageStats storage;
	int64 replicated_hypertable_count;
	int64 child_count;
	int64 compressed_chunk_count;
	int64 compressed_hypertable_count;
	int64 compressed_size;
	int64 compressed_heap_size;
	int64 compressed_indexes_size;
	int64 compressed_toast_size;
	int64 compressed_row_count;
	int64 uncompressed_heap_size;
	int64 uncompressed_indexes_size;
	int64 uncompressed_toast_size;
	int64 uncompressed_row_count;
} HyperStats;

typedef enum StatsRelType
{
	RELTYPE_HYPERTABLE,
	RELTYPE_DISTRIBUTED_HYPERTABLE,
	RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER,
	RELTYPE_REPLICATED_DISTRIBUTED_HYPERTABLE,
	RELTYPE_MATERIALIZED_HYPERTABLE,
	RELTYPE_COMPRESSION_HYPERTABLE,
	RELTYPE_CONTINUOUS_AGG,
	RELTYPE_TABLE,
	RELTYPE_INHERITANCE_TABLE,
	RELTYPE_INDEX,
	RELTYPE_PARTITIONED_TABLE,
	RELTYPE_PARTITIONED_INDEX,
	RELTYPE_PARTITION,
	RELTYPE_FOREIGN_TABLE,
	RELTYPE_VIEW,
	RELTYPE_MATVIEW,
	RELTYPE_CHUNK,
	RELTYPE_DISTRIBUTED_CHUNK,
	RELTYPE_COMPRESSION_CHUNK,
	RELTYPE_MATERIALIZED_CHUNK,
	RELTYPE_OTHER,
} StatsRelType;

typedef struct TelemetryStats
{
	HyperStats hypertables;
	HyperStats distributed_hypertables;
	HyperStats distributed_hypertable_members;
	HyperStats continuous_aggs;
	/* Internal tables */
	HyperStats compression_hypertable;
	HyperStats materialized_hypertable;
	StorageStats tables;
	HyperStats partitioned_tables;
	StorageStats materialized_views;
	BaseStats views;
} TelemetryStats;

extern void ts_telemetry_stats_gather(TelemetryStats *stats);

#endif /* TIMESCALEDB_TELEMETRY_STATS_H */
