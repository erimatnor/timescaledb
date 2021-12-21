/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_STATS_H
#define TIMESCALEDB_TELEMETRY_STATS_H

#include <postgres.h>

typedef enum StatsType
{
	STATS_TYPE_BASE,
	STATS_TYPE_STORAGE,
	STATS_TYPE_HYPER
} StatsType;

typedef struct BaseStats
{
	StatsType type;
	int64 relcount;
} BaseStats;

typedef struct StorageStats
{
	BaseStats base;
	int64 relpages;
	int64 reltuples;
	int64 total_relation_size;
	int64 indexes_size;
} StorageStats;

typedef struct HyperStats
{
	StorageStats storage;
	int64 chunkcount;
	int64 compressed_chunkcount;
	int64 compressed_size;
	int64 compressed_heap_size;
	int64 compressed_indexes_size;
	int64 compressed_toast_size;
	int64 uncompressed_heap_size;
	int64 uncompressed_indexes_size;
	int64 uncompressed_toast_size;
} HyperStats;

typedef enum HyperStatsIndex
{
	STATS_HYPERTABLE,
	STATS_HYPERTABLE_COMPRESSED,
	STATS_DISTRIBUTED_HYPERTABLE,
	STATS_DISTRIBUTED_HYPERTABLE_COMPRESSED,
	STATS_DISTRIBUTED_HYPERTABLE_MEMBER,
	STATS_DISTRIBUTED_HYPERTABLE_MEMBER_COMPRESSED,
	STATS_CONTINUOUS_AGG,
	STATS_CONTINUOUS_AGG_COMPRESSED,
	STATS_PARTITIONED_TABLE,
	STATS_PARTITIONED_INDEX,
	_MAX_STATS_HYPER,
} HyperStatsIndex;

typedef enum StorageStatsIndex
{
	STATS_INDEX,
	STATS_TABLE,
	STATS_MATVIEW,
	_MAX_STATS_STORAGE,
} StorageStatsIndex;

typedef enum BaseStatsIndex
{
	STATS_VIEW,
	STATS_FOREIGN_TABLE,
	_MAX_STATS_BASE,
} BaseStatsIndex;

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

typedef struct AllRelkindStats
{
	HyperStats hyperstats[_MAX_STATS_HYPER];
	StorageStats storagestats[_MAX_STATS_STORAGE];
	BaseStats basestats[_MAX_STATS_BASE];
} AllRelkindStats;

extern void ts_telemetry_relation_stats(AllRelkindStats *stats);

#endif /* TIMESCALEDB_TELEMETRY_STATS_H */
