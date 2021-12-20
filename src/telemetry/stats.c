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
#include "catalog.h"
#include "chunk.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"

typedef struct StatProcessCtx
{
	AllRelkindStats *stats;
    ScanIterator compressed_chunk_stats_iterator;
	bool iterator_valid;
} StatsProcessCtx;

static void
add_storage_stats(StorageStats *stats, Form_pg_class class)
{
	Datum res;

	stats->reltuples += class->reltuples;
	stats->relpages += class->relpages;
	res = DirectFunctionCall2(pg_relation_size,
							  ObjectIdGetDatum(class->oid),
							  CStringGetTextDatum("main"));
	stats->relsize += DatumGetInt64(res);
}

/*
static void
add_distributed_hypertable_size_stats(RelkindStats *stats, Form_pg_class class)
{
	Oid funcid;

	add_relation_size_stats(stats, class);

	OidFunctionCall1(funcid, ObjectIdGetDatum(class->oid));
}
*/
static void
add_chunk_stats(HyperStats *stats, Form_pg_class class, const Chunk *chunk, const Form_compression_chunk_size fd_compr)
{
	add_storage_stats(&stats->storage, class);
	stats->chunkcount++;

	if (ts_chunk_is_compressed(chunk))
		stats->compressed_chunkcount++;

	if (NULL != fd_compr)
	{
		stats->compressed_heap_size += fd_compr->compressed_heap_size;
		stats->compressed_index_size += fd_compr->compressed_index_size;
		stats->compressed_toast_size += fd_compr->compressed_toast_size;		
		stats->uncompressed_heap_size += fd_compr->uncompressed_heap_size;
		stats->uncompressed_index_size += fd_compr->uncompressed_index_size;
		stats->uncompressed_toast_size += fd_compr->uncompressed_toast_size;		
	}
}

static void
process_relation_stats(BaseStats *stats, Form_pg_class class)
{
	stats->relcount++;

	if (RELKIND_HAS_STORAGE(class->relkind))
		add_storage_stats((StorageStats *) stats, class);
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
process_chunk_stats(StatsProcessCtx *statsctx, Form_pg_class class, const Chunk *chunk, Cache *htcache)
{
	AllRelkindStats *stats = statsctx->stats;
	const Hypertable *ht;

	/* Lookup the chunk's parent hypertable */
	ht = ts_hypertable_cache_get_entry(htcache, chunk->hypertable_relid, CACHE_FLAG_NONE);

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		/* This is an internal compression table, but could be for a regular
		 * hypertable or for an internal materialized hypertable (cagg). The
		 * latter case is currently not handled */
		add_chunk_stats(&stats->hyperstats[STATS_HYPER_HYPERTABLE_COMPRESSED], class, chunk, NULL);
	}
	else
	{
		switch (ht->fd.replication_factor)
		{
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				add_chunk_stats(&stats->hyperstats[STATS_HYPER_DISTRIBUTED_HYPERTABLE_MEMBER],
								class,
								chunk, NULL);
				break;
			case HYPERTABLE_REGULAR:
			{
				/* Not dealing with an internal compression hypertable, but could be a
				 * materialized hypertable (cagg) */
				const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);

				if (cagg)
					add_chunk_stats(&stats->hyperstats[STATS_HYPER_CONTINUOUS_AGG], class, chunk, NULL);
				else 
				{
					bool found_compression_stats = false;
					
					if (ts_chunk_is_compressed(chunk))
					{
						
						elog(NOTICE, "Finding compression stats for chunk %d", chunk->fd.id);
						ts_scan_iterator_reset(&statsctx->compressed_chunk_stats_iterator);
						ts_scan_iterator_scan_key_init(&statsctx->compressed_chunk_stats_iterator,
													   Anum_compression_chunk_size_pkey_chunk_id,
													   BTEqualStrategyNumber,
													   F_INT4EQ,
													   Int32GetDatum(chunk->fd.id));
						
						if (statsctx->iterator_valid)
							ts_scan_iterator_rescan(&statsctx->compressed_chunk_stats_iterator);
						else
						{
							ts_scan_iterator_start_scan(&statsctx->compressed_chunk_stats_iterator);
							statsctx->iterator_valid = true;
						}
						
						if (ts_scan_iterator_next(&statsctx->compressed_chunk_stats_iterator))
						{
							Form_compression_chunk_size fd;
							bool should_free;
							HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&statsctx->compressed_chunk_stats_iterator, false, &should_free);

							fd = (Form_compression_chunk_size) GETSTRUCT(tuple);
							
							add_chunk_stats(&stats->hyperstats[STATS_HYPER_HYPERTABLE], class, chunk, fd);
							
							if (should_free)
								heap_freetuple(tuple);

							found_compression_stats = true;
							elog(NOTICE, "Found compression stats for chunk %d", chunk->fd.id);
						}
						else
						{
							elog(NOTICE, "chunk compression stats not found for compressed chunk %d", chunk->fd.id);
							statsctx->iterator_valid = false;
						}
					}

					if (!found_compression_stats)
						add_chunk_stats(&stats->hyperstats[STATS_HYPER_HYPERTABLE], class, chunk, NULL);
				}
				break;
			}
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

static void
process_table_stats(StatsProcessCtx *statsctx, Form_pg_class class, Cache *htcache)
{
	AllRelkindStats *stats = statsctx->stats;
	
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
			if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
			{
				/* This is an internal compression table, but could be for a
				 * regular hypertable, a distributed member hypertable, or for
				 * an internal materialized hypertable (cagg). The latter case
				 * is currently not handled */
				process_relation_stats((BaseStats *) &stats->hyperstats[STATS_HYPER_HYPERTABLE_COMPRESSED],
									   class);
			}
			else
			{
				/* Not dealing with an internal compression hypertable, but
				 * could be a materialized hypertable (cagg).  */
				switch (ht->fd.replication_factor)
				{
					case HYPERTABLE_DISTRIBUTED_MEMBER:
						process_relation_stats((BaseStats *) &stats->hyperstats
												   [STATS_HYPER_DISTRIBUTED_HYPERTABLE_MEMBER],
											   class);
						break;
					case HYPERTABLE_REGULAR:
					{
						const ContinuousAgg *cagg =
							ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);

						/* Don't count internal materialized hypertables here
						 * (caggs), since those are processed when the main view
						 * is processed. */
						if (!cagg)
							process_relation_stats((BaseStats *) &stats->hyperstats[STATS_HYPER_HYPERTABLE],
												   class);
						break;
					}
					case HYPERTABLE_DISTRIBUTED:
						process_relation_stats((BaseStats *) &stats->hyperstats[STATS_HYPER_DISTRIBUTED_HYPERTABLE],
											   class);
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
	else
	{
		/* Check if it is a chunk */
		const Chunk *chunk = ts_chunk_get_by_relid(class->oid, false);

		if (chunk)
			process_chunk_stats(statsctx, class, chunk, htcache);
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
			add_chunk_stats(&stats->hyperstats[STATS_HYPER_HYPERTABLE], class, chunk, NULL);

			/* Normally, the count of the number of compressed chunks comes
			 * from counting the chunks in the internal compression
			 * hypertable. But a distributed hypertable has no internal
			 * compression table on the access node, so, for this case, we
			 * handle the count of compressed chunks based on the reported
			 * compression status on the main table chunk instead. */
			if (ts_chunk_is_compressed(chunk))
				stats->hyperstats[STATS_HYPER_DISTRIBUTED_HYPERTABLE_COMPRESSED].chunkcount++;
		}
		else
			process_relation_stats(&stats->basestats[STATS_BASE_FOREIGN_TABLE], class);
	}
}

static void
process_view_stats(AllRelkindStats *stats, Form_pg_class class, Cache *htcache)
{
	const ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(class->oid);

	if (cagg)
		process_relation_stats((BaseStats *) &stats->hyperstats[STATS_HYPER_CONTINUOUS_AGG], class);

	/* TODO: filter internal cagg views */
}

static bool should_ignore_relation(Form_pg_class class)
{
	return (class->relnamespace == PG_CATALOG_NAMESPACE ||
			class->relnamespace == PG_TOAST_NAMESPACE || isAnyTempNamespace(class->relnamespace) ||
			ts_is_catalog_table(class->oid));
}

static void
stats_init(AllRelkindStats *stats)
{
	int i;

	MemSet(stats, 0, sizeof(*stats));

	for (i = 0; i < STATS_BASE_MAX; i++)
		stats->basestats[i].type = STATS_TYPE_BASE;

	for (i = 0; i < STATS_STORAGE_MAX; i++)
		stats->storagestats[i].base.type = STATS_TYPE_STORAGE;

	for (i = 0; i < STATS_HYPER_MAX; i++)
		stats->hyperstats[i].storage.base.type = STATS_TYPE_HYPER;
}

static BaseStats *
get_stats(AllRelkindStats *stats, StatsType type, unsigned index)
{
	BaseStats *res;

	switch (type)
	{
		case STATS_TYPE_BASE:
			Assert(index < STATS_BASE_MAX);
			res = &stats->basestats[index];
			break;
		case STATS_TYPE_STORAGE:
			Assert(index < STATS_STORAGE_MAX);
			res = (BaseStats *) &stats->storagestats[index];
			break;
		case STATS_TYPE_HYPER:
			Assert(index < STATS_HYPER_MAX);
			res = (BaseStats *) &stats->hyperstats[index];
			break;
	}

	return res;
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
	ScanIterator iterator;
	StatsProcessCtx statsctx = {
		.stats = stats,
		.iterator_valid = false,
	};

	stats_init(stats);
	iterator = ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE,
									   AccessShareLock,
									   CurrentMemoryContext);
	ts_scan_iterator_set_index(&iterator, COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	statsctx.compressed_chunk_stats_iterator = iterator;
	
	rel = table_open(RelationRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ClassOidIndexId, false, NULL, 0, NULL);

	relmcxt = AllocSetContextCreate(CurrentMemoryContext, "RelationStats", ALLOCSET_DEFAULT_SIZES);

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
				process_table_stats(&statsctx, class, htcache);
				break;
			case RELKIND_INDEX:
				process_relation_stats(get_stats(stats, STATS_TYPE_STORAGE, STATS_STORAGE_INDEX),
									   class);
				break;
			case RELKIND_FOREIGN_TABLE:
				process_foreign_table_stats(stats, class);
				break;
			case RELKIND_PARTITIONED_TABLE:
				process_relation_stats(get_stats(stats,
												 STATS_TYPE_HYPER,
												 STATS_HYPER_PARTITIONED_TABLE),
									   class);
				break;
			case RELKIND_PARTITIONED_INDEX:
				process_relation_stats(get_stats(stats,
												 STATS_TYPE_HYPER,
												 STATS_HYPER_PARTITIONED_INDEX),
									   class);
				break;
			case RELKIND_VIEW:
				process_view_stats(stats, class, htcache);
				break;
			case RELKIND_MATVIEW:
				process_relation_stats(get_stats(stats, STATS_TYPE_STORAGE, STATS_STORAGE_MATVIEW),
									   class);
				break;
			default:
				/* RELKIND we don't care about */
				break;
		}
	}

	MemoryContextSwitchTo(oldmcxt);
	systable_endscan(scan);
	table_close(rel, AccessShareLock);
	ts_scan_iterator_close(&statsctx.compressed_chunk_stats_iterator);
	ts_cache_release(htcache);
	MemoryContextDelete(relmcxt);
}
