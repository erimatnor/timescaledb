/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_STATS_H
#define TIMESCALEDB_TELEMETRY_STATS_H

#include <postgres.h>

typedef struct RelkindStats
{
	int64 relcount;
	int64 relpages;
	int64 reltuples;
	int64 relsize;
	int64 chunkcount;
} RelkindStats;

typedef struct AllRelkindStats
{
	RelkindStats hypertable;
	RelkindStats distributed_hypertable;
	RelkindStats distributed_hypertable_member;
	RelkindStats continuous_agg;
	RelkindStats hypertable_compressed;
	RelkindStats distributed_hypertable_compressed;
	RelkindStats distributed_hypertable_member_compressed;
	RelkindStats continuous_agg_compressed;
	RelkindStats table;
	RelkindStats index;
	RelkindStats view;
	RelkindStats matview;
	RelkindStats foreign_table;
	RelkindStats partitioned_table;
	RelkindStats partitioned_index;
} AllRelkindStats;

extern void ts_telemetry_relation_stats(AllRelkindStats *stats);

#endif /* TIMESCALEDB_TELEMETRY_STATS_H */
