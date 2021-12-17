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
	int64 relsize;
} StorageStats;

typedef struct HyperStats
{
	StorageStats storage;
	int64 chunkcount;
	int64 compressed_chunkcount;
} HyperStats;

typedef enum HyperStatsIndex
{
	STATS_HYPER_HYPERTABLE,
	STATS_HYPER_HYPERTABLE_COMPRESSED,
	STATS_HYPER_DISTRIBUTED_HYPERTABLE,
	STATS_HYPER_DISTRIBUTED_HYPERTABLE_COMPRESSED,
	STATS_HYPER_DISTRIBUTED_HYPERTABLE_MEMBER,
	STATS_HYPER_DISTRIBUTED_HYPERTABLE_MEMBER_COMPRESSED,
	STATS_HYPER_CONTINUOUS_AGG,
	STATS_HYPER_CONTINUOUS_AGG_COMPRESSED,
	STATS_HYPER_PARTITIONED_TABLE,
	STATS_HYPER_PARTITIONED_INDEX,
	STATS_HYPER_MAX,
} HyperStatsIndex;

typedef enum StorageStatsIndex
{
	STATS_STORAGE_INDEX,
	STATS_STORAGE_TABLE,
	STATS_STORAGE_MATVIEW,
	STATS_STORAGE_MAX,
} StorageStatsIndex;

typedef enum BaseStatsIndex
{
	STATS_BASE_VIEW,
	STATS_BASE_FOREIGN_TABLE,
	STATS_BASE_MAX,
} BaseStatsIndex;

typedef struct AllRelkindStats
{
	HyperStats hyperstats[STATS_HYPER_MAX];
	StorageStats storagestats[STATS_STORAGE_MAX];
	BaseStats basestats[STATS_BASE_MAX];
} AllRelkindStats;

extern void ts_telemetry_relation_stats(AllRelkindStats *stats);

#endif /* TIMESCALEDB_TELEMETRY_STATS_H */
