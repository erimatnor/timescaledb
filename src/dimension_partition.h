/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DIMENSION_PARTITION_H
#define TIMESCALEDB_DIMENSION_PARTITION_H

#include <postgres.h>
#include <lib/rbtree.h>

#include "scan_iterator.h"

typedef struct DimensionPartition
{
	RBTNode rbtnode;
	int32 id;
	int32 dimension_id;
	int64 range_start;
	int64 range_end;
	List *data_nodes;
} DimensionPartition;

extern ScanIterator ts_dimension_partition_scan_iterator_create(MemoryContext result_mcxt);
extern void ts_dimension_partition_scan_iterator_set_id(ScanIterator *it,
														int32 dimension_partition_id,
														const ScanTupLock *tuplock);
extern void ts_dimension_partition_scan_iterator_set_dimension_id(ScanIterator *it, int32 dimension_id,
																  const ScanTupLock *tuplock);
extern RBTree *ts_dimension_partition_get_all_as_rbtree(int32 dimension_id, Oid dimtype);
extern const DimensionPartition *ts_dimension_partition_find(RBTree *rbt, int64 coord);
extern RBTree *ts_dimension_partition_recreate_partitioning(int32 dimension_id, unsigned int num_partitions,
															List *data_nodes, int replication_factor);

#endif /* TIMESCALEDB_DIMENSION_PARTITION_H */
