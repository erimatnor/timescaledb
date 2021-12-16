/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/table.h>
#include <access/genam.h>
#include <access/tableam.h>
#include <access/htup_details.h>
#include <catalog/pg_class.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_namespace.h>
#include <utils/builtins.h>
#include <fmgr.h>

#include "stats.h"
#include "chunk.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"

static void
add_relation_size_stats(RelkindStats *stats, Form_pg_class class)
{
	stats->reltuples += class->reltuples;
	stats->relpages += class->relpages;
	
	if (RELKIND_HAS_STORAGE(class->relkind))
	{
		Datum res;
		
		res = DirectFunctionCall2(pg_relation_size, ObjectIdGetDatum(class->oid), CStringGetTextDatum("main"));
		stats->relsize += DatumGetInt64(res);
	}
}

static void
add_chunk_stats(RelkindStats *stats, Form_pg_class class, bool chunk_holds_data)
{
	if (chunk_holds_data)
		add_relation_size_stats(stats, class);
	stats->chunkcount += 1;
}

static void
process_relation_stats(RelkindStats *stats, Form_pg_class class)
{
	add_relation_size_stats(stats, class);
	stats->relcount += 1;
}

static void
process_hypertable_stats(RelkindStats *stats, Form_pg_class class, const Hypertable *ht)
{
	/* Don't count internal hypertables that are used for compression or
	 * continuous aggregates */
	if (!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);
		
		if (NULL == cagg)
			stats->relcount++;
	}
}

/*
 * Process a relation identified as being a chunk.
 *
 * The chunk could be part of a
 *
 *  - Hypertable
 *  - Distributed hypertable
 *  - Distributed hypertable member
 *  - Materialized hypertable (cagg) chunk
 *  - Internal compression table for hypertable
 *  - Internal compression table for materialized hypertable (cagg)
 */
static void
process_chunk_stats(AllRelkindStats *stats, Form_pg_class class, const Chunk *chunk, Cache *htcache)
{
	const Hypertable *ht;
	
	/* Lookup the chunk's parent hypertable */
	ht = ts_hypertable_cache_get_entry(htcache, chunk->hypertable_relid, CACHE_FLAG_NONE);

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		/* This is an internal compression table, but could be for a regular
		 * hypertable or for an internal materialized hypertable (cagg). The
		 * latter case is currently not handled */				
		add_chunk_stats(&stats->hypertable_compressed, class, true);
	}
	else
	{
		/* Not dealing with an internal compression hypertable, but could be a
		 * materialized hypertable (cagg) */
		const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);
		
		if (cagg)
			add_chunk_stats(&stats->continuous_agg, class, ht);
		else
		{
			switch (ht->fd.replication_factor)
			{
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				add_chunk_stats(&stats->distributed_hypertable_member, class, false);
				break;
			case HYPERTABLE_REGULAR:
				add_chunk_stats(&stats->hypertable, class, false);
				break;
			case HYPERTABLE_DISTRIBUTED:
				/* This case is handled when processing foreign tables. It can
				 * be handled more efficiently there. */
				Assert(false);			   
				break;
			default:
				Assert(ht->fd.replication_factor >= 1);
				Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
				/* TODO: handle replicated hypertables */
				break;
			}
		}
	}
}

static void
process_table_stats(AllRelkindStats *stats, Form_pg_class class, Cache *htcache)
{
	/* Check things that are already in the pg_class tuple first before
	 * further lookups */
	if (class->relispartition)
	{
		/* Partition in a partitioned table */
	}
	else if (class->relhassubclass)
	{
		/* Could be a hypertable */
		const Hypertable *ht;

		ht = ts_hypertable_cache_get_entry(htcache, class->oid, CACHE_FLAG_MISSING_OK);

		if (ht)
		{
			switch (ht->fd.replication_factor)
			{
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				stats->distributed_hypertable_member.relcount++;
				break;
			case HYPERTABLE_REGULAR:
				process_hypertable_stats(&stats->hypertable, class, ht);
				break;
			case HYPERTABLE_DISTRIBUTED:
				stats->distributed_hypertable.relcount++;
				break;
			default:
				Assert(ht->fd.replication_factor >= 1);
				Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
				/* TODO: handle replicated hypertables */
				
				break;
			}
		}
	}
	else 
	{
		/* Check if it is a chunk */
		const Chunk *chunk = ts_chunk_get_by_relid(class->oid, false);
		
		if (chunk)
			process_chunk_stats(stats, class, chunk, htcache);
	}
}

static void
process_foreign_table_stats(AllRelkindStats *stats, Form_pg_class class)
{
	if (class->relispartition)
	{
		/* Partition in a partitioned table */
	}
	else
	{
		/* Check if it is a chunk in a distributed hypertable */
		const Chunk *chunk = ts_chunk_get_by_relid(class->oid, false);
		
		if (chunk)
		{
			add_chunk_stats(&stats->distributed_hypertable, class, false);

			/* Normally, the count of the number of compressed chunks comes
			 * from counting the chunks in the internal compression
			 * hypertable. But a distributed hypertable has no internal
			 * compression table on the access node, so, for this case, we
			 * handle the count of compressed chunks based on the reported
			 * compression status on the main table chunk instead. */
			if (ts_chunk_is_compressed(chunk))
				stats->distributed_hypertable_compressed.chunkcount++;
		}
		else
			process_relation_stats(&stats->foreign_table, class);
	}
}

static void
process_view_stats(AllRelkindStats *stats, Form_pg_class class, Cache *htcache)
{
	const ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(class->oid);

	if (cagg)
		process_relation_stats(&stats->continuous_agg, class);
}

static bool
should_ignore_relation(Form_pg_class class)
{
	return (class->relnamespace == PG_CATALOG_NAMESPACE ||
			class->relnamespace == PG_TOAST_NAMESPACE ||
			isAnyTempNamespace(class->relnamespace) || 
			ts_is_catalog_table(class->oid));
}

/*
 * Scan the entire pg_class catalog table for all relations. For each
 * relation, classify it and gather basic stats.
 */
void
ts_telemetry_relation_stats(AllRelkindStats *stats)
{
	Relation rel;
	SysScanDesc scan;
	Cache *htcache = ts_hypertable_cache_pin();
	MemoryContext oldmcxt, relmcxt;
	
	MemSet(stats, 0, sizeof(*stats));
	
	rel = table_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ClassOidIndexId, false,
							  NULL, 0, NULL);

	relmcxt = AllocSetContextCreate(CurrentMemoryContext,
									"RelationStats",
									ALLOCSET_DEFAULT_SIZES);

	/* Use temporary per-tuple memory context to not accumulate cruft during
	 * processing of pg_class */
	oldmcxt = MemoryContextSwitchTo(relmcxt);
	
	while (true)
	{
		HeapTuple tup;   
		Form_pg_class class;

		MemoryContextReset(relmcxt);
		tup = systable_getnext(scan);

		if (!HeapTupleIsValid(tup))
			break;
	   
		class = (Form_pg_class) GETSTRUCT(tup);

		if (should_ignore_relation(class))
			continue;
		
		switch (class->relkind)
		{
		case RELKIND_RELATION:
			process_table_stats(stats, class, htcache);
			break;
		case RELKIND_INDEX:
			process_relation_stats(&stats->index, class);
			break;
		case RELKIND_FOREIGN_TABLE:
			process_foreign_table_stats(stats, class);
			break;
		case RELKIND_PARTITIONED_TABLE:
			process_relation_stats(&stats->partitioned_table, class);
			break;
		case RELKIND_PARTITIONED_INDEX:
			process_relation_stats(&stats->partitioned_index, class);
			break;
		case RELKIND_VIEW:			
			process_view_stats(stats, class, htcache);
			break;
		case RELKIND_MATVIEW:			
			process_relation_stats(&stats->matview, class);
			break;
		default:
			/* RELKIND we don't care about */
			break;
		}
	}

	MemoryContextSwitchTo(oldmcxt);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	ts_cache_release(htcache);
	MemoryContextDelete(relmcxt);
}
