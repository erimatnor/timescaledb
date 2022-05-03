/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <catalog/pg_type_d.h>
#include <postgres.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/catalog.h>
#include <commands/tablecmds.h>
#include <nodes/parsenodes.h>
#include <utils/array.h>
#include <utils/palloc.h>
#include <utils/rel.h>

#include "ts_catalog/catalog.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_partition.h"
#include "scanner.h"

#include "compat/compat.h"


ScanIterator
ts_dimension_partition_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(DIMENSION_PARTITION, AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

void
ts_dimension_partition_scan_iterator_set_id(ScanIterator *it, int32 dimension_partition_id,
											const ScanTupLock *tuplock)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(), DIMENSION_PARTITION, DIMENSION_PARTITION_ID_IDX);
	ts_scan_iterator_scan_key_reset(it);   
	ts_scan_iterator_scan_key_init(it,
								   Anum_dimension_partition_id_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(dimension_partition_id));
	it->ctx.tuplock = tuplock;
}

void
ts_dimension_partition_scan_iterator_set_dimension_id(ScanIterator *it, int32 dimension_id,
													  const ScanTupLock *tuplock)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(), DIMENSION_PARTITION,
									  DIMENSION_PARTITION_DIMENSION_ID_RANGE_START_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_dimension_partition_dimension_id_range_start_idx_dimension_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(dimension_id));
	it->ctx.tuplock = tuplock;
}

static int
dimpart_cmp(const RBTNode *a, const RBTNode *b, void *arg)
{
	const DimensionPartition *dp_a = (const DimensionPartition *) a;
	const DimensionPartition *dp_b = (const DimensionPartition *) b;

	if (dp_a->range_start >= dp_b->range_start &&
		dp_a->range_end < dp_b->range_end)
		return 0;

	return dp_a->range_start - dp_b->range_start;
}

static void
dimpart_combine(RBTNode *existing, const RBTNode *newdata, void *arg)
{
	elog(ERROR, "conflicting dimension partitions");
}

static RBTNode *
dimpart_alloc(void *arg)
{
	return palloc(sizeof(DimensionPartition));;
}

static void
dimpart_free(RBTNode *x, void *arg)
{
	pfree(x);
}

static DimensionPartition *
dimpart_create(const Form_dimension_partition fd, Oid dimtype, bool data_nodes_isnull)
{
	DimensionPartition *dp = palloc(sizeof(DimensionPartition));
	
	dp->id = fd->id;
	dp->dimension_id = fd->dimension_id;
	dp->range_start = fd->range_start;
	dp->range_end = ts_time_get_noend_or_max(dimtype);
	dp->data_nodes = NIL;

	if (!data_nodes_isnull)
	{
		ArrayIterator arrit;
		bool isnull = false;
		Datum elem = (Datum) NULL;
		
		arrit = array_create_iterator(fd->data_nodes, 1, NULL);
		
		while (array_iterate(arrit, &elem, &isnull))
		{
			if (!isnull)
			{
				const Name dn = DatumGetName(elem);
				char *dn_name = pstrdup(NameStr(*dn));
				
				dp->data_nodes = lappend(dp->data_nodes, dn_name);
			}
		}
		
		array_free_iterator(arrit);
	}
	
	return dp;
}

RBTree *
ts_dimension_partition_get_all_as_rbtree(int32 dimension_id, Oid dimtype)
{
	ScanIterator it;
	DimensionPartition *prev_dp = NULL;
	RBTree *tree;
	bool first = true;

	it = ts_dimension_partition_scan_iterator_create(CurrentMemoryContext);
	ts_dimension_partition_scan_iterator_set_dimension_id(&it, dimension_id, NULL);

	tree = rbt_create(sizeof(DimensionPartition),
					  dimpart_cmp,
					  dimpart_combine,
					  dimpart_alloc,
					  dimpart_free,
					  NULL);

	ts_scanner_foreach(&it)
	{
		bool should_free = false;
		HeapTuple tup = ts_scan_iterator_fetch_heap_tuple(&it, false, &should_free);
		const Form_dimension_partition fd = (Form_dimension_partition) GETSTRUCT(tup);
		bool data_nodes_isnull = slot_attisnull(ts_scan_iterator_slot(&it), Anum_dimension_partition_data_nodes);
		DimensionPartition *dp;

		dp = dimpart_create(fd, dimtype, data_nodes_isnull);
		
		if (!first)
		{
			bool is_new = false;
			
			prev_dp->range_end = dp->range_start;
			rbt_insert(tree, &prev_dp->rbtnode, &is_new);			
			pfree(prev_dp);
		}

		first = false;
		prev_dp = dp;
	}

	ts_scan_iterator_close(&it);
	
	if (NULL != prev_dp)
	{
		bool is_new = false;
		rbt_insert(tree, &prev_dp->rbtnode, &is_new);		
		pfree(prev_dp);
	}

	return tree;
}

const DimensionPartition *
ts_dimension_partition_find(RBTree *rbt, int64 coord)
{
	DimensionPartition dp_point = {
		.range_start = coord,
		.range_end = coord,
	};
    const DimensionPartition *dp_found = (DimensionPartition *) rbt_find(rbt, &dp_point.rbtnode);

	Assert(dp_found->range_start <= coord);
	Assert(dp_found->range_end > coord);

	return dp_found;	
}

static List *
get_replica_nodes(List *data_nodes, unsigned int index, int replication_factor)
{
	List *replica_nodes = NIL;
	int i;
	
	/* Check for single-node case */
	if (data_nodes == NIL)
		return NIL;

	for (i = 0; i < replication_factor; i++)
	{
		int list_index = (index + i) % list_length(data_nodes);
		replica_nodes = lappend(replica_nodes, list_nth(data_nodes, list_index));		
	}

	return replica_nodes;
}

static HeapTuple
create_dimension_partition_tuple(Relation rel, const DimensionPartition *dp)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	Datum values[Natts_dimension_partition];
	bool nulls[Natts_dimension_partition] = { false };
	int i = 0;

	values[AttrNumberGetAttrOffset(Anum_dimension_partition_id)] = Int32GetDatum(dp->id);
	values[AttrNumberGetAttrOffset(Anum_dimension_partition_dimension_id)] = Int32GetDatum(dp->dimension_id);
	values[AttrNumberGetAttrOffset(Anum_dimension_partition_range_start)] = Int64GetDatum(dp->range_start);
	
	if (dp->data_nodes == NIL)
	{
		nulls[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)] = true;
	}
	else
	{
		Datum *dn_datums = palloc(sizeof(Datum) * list_length(dp->data_nodes));
		ArrayType *dn_arr;	
		ListCell *lc;
		
		foreach (lc, dp->data_nodes)
		{
			const char *dn = lfirst(lc);
			dn_datums[i] = CStringGetDatum(dn);
		}
		
		dn_arr = construct_array(dn_datums, list_length(dp->data_nodes),
								 NAMEOID, NAMEDATALEN, false, TYPALIGN_CHAR);
		values[AttrNumberGetAttrOffset(Anum_dimension_partition_data_nodes)] = PointerGetDatum(dn_arr);
	}
	
	return heap_form_tuple(tupdesc, values, nulls);
}

RBTree *
ts_dimension_partition_recreate_partitioning(int32 dimension_id, unsigned int num_partitions, List *data_nodes, int replication_factor)	
{
	int64 partition_size = DIMENSION_SLICE_CLOSED_MAX / ((int64) num_partitions);
	int64 range_start = DIMENSION_SLICE_MINVALUE;
	Catalog *catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, DIMENSION_PARTITION);
	List *relids_logged = NIL;
	RBTree *tree;
	Relation rel;
    unsigned int i;

	Assert(num_partitions > 0);
	Assert(replication_factor >= 0);
	Assert(data_nodes == NIL || replication_factor > 0);

	elog(NOTICE, "partition size " INT64_FORMAT, partition_size);
	
	rel = table_open(relid, AccessExclusiveLock);

	if (RelationIsLogicallyLogged(rel))
		relids_logged = lappend_oid(relids_logged, relid);
	
	ExecuteTruncateGuts(list_make1(rel), list_make1_oid(relid), relids_logged,
					   DROP_RESTRICT, true);

	tree = rbt_create(sizeof(DimensionPartition),
					  dimpart_cmp,
					  dimpart_combine,
					  dimpart_alloc,
					  dimpart_free,
					  NULL);

	for (i = 0; i < num_partitions; i++)
	{
		bool is_new = false;
		int64 range_end = (i == (num_partitions - 1)) ? DIMENSION_SLICE_CLOSED_MAX : range_start + partition_size;
		DimensionPartition dp = {
			.id = ts_catalog_table_next_seq_id(catalog, DIMENSION_PARTITION),
			.dimension_id = dimension_id,
			.range_start = range_start,
			.range_end = range_end,
			.data_nodes = get_replica_nodes(data_nodes, i, replication_factor),
		};
		HeapTuple tuple = create_dimension_partition_tuple(rel, &dp);			  	   
		ts_catalog_insert_only(rel, tuple);
		heap_freetuple(tuple);
		rbt_insert(tree, &dp.rbtnode, &is_new);

		/* Hash values for space partitions are in range 0 to INT32_MAX, so
		 * the first partition covers 0 to partition size (although the start
		 * value is -INF) */
		if (range_start == DIMENSION_SLICE_MINVALUE)
			range_start = 0;
		
		range_start += partition_size;
	}
		
	table_close(rel, NoLock);

	/* Make changes visible */
	CommandCounterIncrement();
	
	return tree;
}
