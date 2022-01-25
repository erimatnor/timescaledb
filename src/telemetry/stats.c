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
#include "extension.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"

typedef struct StatProcessCtx
{
	AllRelkindStats *stats;
	ScanIterator compressed_chunk_stats_iterator;
	bool iterator_valid;
} StatsProcessCtx;

static StatsRelType
classify_hypertable(const Hypertable *ht)
{
	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		/* This is an internal compression table, but could be for a
		 * regular hypertable, a distributed member hypertable, or for
		 * an internal materialized hypertable (cagg). The latter case
		 * is currently not handled */
		return RELTYPE_COMPRESSION_HYPERTABLE;
	}
	else
	{
		/* Not dealing with an internal compression hypertable, but
		 * could be a materialized hypertable (cagg) unless it is
		 * distributed.  */
		switch (ht->fd.replication_factor)
		{
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				return RELTYPE_DISTRIBUTED_HYPERTABLE;
			case HYPERTABLE_REGULAR:
			{
				const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);

				if (cagg)
					return RELTYPE_MATERIALIZED_HYPERTABLE;

				return RELTYPE_HYPERTABLE;
			}
			case HYPERTABLE_DISTRIBUTED:
				return RELTYPE_DISTRIBUTED_HYPERTABLE;
			default:
				Assert(ht->fd.replication_factor > 1);
				return RELTYPE_REPLICATED_DISTRIBUTED_HYPERTABLE;
		}
	}
}

static StatsRelType
classify_inheritance_table(Oid relid, Cache *htcache, const Hypertable **ht)
{
	/* Check if this is a hypertable */
	*ht = ts_hypertable_cache_get_entry(htcache, relid, CACHE_FLAG_MISSING_OK);

	if (*ht)
		return classify_hypertable(*ht);

	return RELTYPE_INHERITANCE_TABLE;
}

static StatsRelType
classify_table(Form_pg_class class, Cache *htcache, const Hypertable **ht, const Chunk **chunk)
{
	Assert(class->relkind == RELKIND_RELATION);

	if (class->relispartition)
		return RELTYPE_PARTITION;
	else if (class->relhassubclass)
		return classify_inheritance_table(class->oid, htcache, ht);
	else
	{
		/* Check if it is a chunk */
		*chunk = ts_chunk_get_by_relid(class->oid, false);

		if (*chunk)
		{
			if (class->relkind == RELKIND_FOREIGN_TABLE)
				return RELTYPE_DISTRIBUTED_CHUNK;
			return RELTYPE_CHUNK;
		}
	}

	return RELTYPE_TABLE;
}

static StatsRelType
classify_foreign_table(Oid relid, const Chunk **chunk)
{
	*chunk = ts_chunk_get_by_relid(relid, false);

	if (*chunk)
		return RELTYPE_DISTRIBUTED_CHUNK;

	return RELTYPE_FOREIGN_TABLE;
}

static StatsRelType
classify_view(Oid relid, Cache *htcache)
{
	const ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(relid);

	if (cagg)
		return RELTYPE_CONTINUOUS_AGG;

	/* Ignore internal cagg views, so classify as other */
	cagg = ts_continuous_agg_find_by_view_name("", "", ContinuousAggPartialView);

	if (cagg)
		return RELTYPE_OTHER;

	cagg = ts_continuous_agg_find_by_view_name("", "", ContinuousAggDirectView);

	if (cagg)
		return RELTYPE_OTHER;

	return RELTYPE_VIEW;
}

static StatsRelType
classify_relation(Form_pg_class class, Cache *htcache, const Hypertable **ht, const Chunk **chunk)
{
	*chunk = NULL;
	*ht = NULL;

	switch (class->relkind)
	{
		case RELKIND_INDEX:
			return RELTYPE_INDEX;
		case RELKIND_RELATION:
			return classify_table(class, htcache, ht, chunk);
		case RELKIND_FOREIGN_TABLE:
			return classify_foreign_table(class->oid, chunk);
		case RELKIND_PARTITIONED_TABLE:
			return RELTYPE_PARTITIONED_TABLE;
		case RELKIND_PARTITIONED_INDEX:
			return RELTYPE_PARTITIONED_INDEX;
		case RELKIND_MATVIEW:
			return RELTYPE_MATVIEW;
		case RELKIND_VIEW:
			return classify_view(class->oid, htcache);
		default:
			return RELTYPE_OTHER;
	}
}

static void
add_storage_stats(StorageStats *stats, Form_pg_class class)
{
	Datum relsize;

	stats->reltuples += class->reltuples;
	stats->relpages += class->relpages;
	relsize = DirectFunctionCall1(pg_total_relation_size, ObjectIdGetDatum(class->oid));
	stats->total_relation_size += DatumGetInt64(relsize);
	relsize = DirectFunctionCall1(pg_indexes_size, ObjectIdGetDatum(class->oid));
	stats->indexes_size += DatumGetInt64(relsize);
}

static void
process_relation_stats(BaseStats *stats, Form_pg_class class)
{
	stats->relcount++;

	if (RELKIND_HAS_STORAGE(class->relkind))
		add_storage_stats((StorageStats *) stats, class);
}

static void
process_distributed_hypertable_stats(BaseStats *stats, Form_pg_class class)
{
	stats->relcount++;
}

static void
add_chunk_stats(HyperStats *stats, Form_pg_class class, const Chunk *chunk,
				const Form_compression_chunk_size fd_compr)
{
	stats->chunkcount++;

	if (RELKIND_HAS_STORAGE(class->relkind))
		add_storage_stats(&stats->storage, class);

	if (ts_chunk_is_compressed(chunk))
		stats->compressed_chunkcount++;

	if (NULL != fd_compr)
	{
		stats->compressed_heap_size += fd_compr->compressed_heap_size;
		stats->compressed_indexes_size += fd_compr->compressed_index_size;
		stats->compressed_toast_size += fd_compr->compressed_toast_size;
		stats->uncompressed_heap_size += fd_compr->uncompressed_heap_size;
		stats->uncompressed_indexes_size += fd_compr->uncompressed_index_size;
		stats->uncompressed_toast_size += fd_compr->uncompressed_toast_size;

		/* Also add compressed sizes to total number for entire table */
		stats->storage.indexes_size += fd_compr->compressed_index_size;
		stats->storage.total_relation_size +=
			fd_compr->compressed_heap_size + fd_compr->compressed_toast_size;
	}
}

static bool
get_chunk_compression_stats(StatsProcessCtx *statsctx, const Chunk *chunk,
							Form_compression_chunk_size compr_stats)
{
	if (!ts_chunk_is_compressed(chunk))
		return false;

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
		HeapTuple tuple =
			ts_scan_iterator_fetch_heap_tuple(&statsctx->compressed_chunk_stats_iterator,
											  false,
											  &should_free);

		fd = (Form_compression_chunk_size) GETSTRUCT(tuple);
		memcpy(compr_stats, fd, sizeof(*fd));

		if (should_free)
			heap_freetuple(tuple);

		return true;
	}

	/* Shouldn't really get here */
	statsctx->iterator_valid = false;

	return false;
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
process_chunk_stats(StatsProcessCtx *statsctx, Form_pg_class class, const Chunk *chunk,
					Cache *htcache)
{
	AllRelkindStats *stats = statsctx->stats;
	const Hypertable *ht;
	StatsRelType reltype;

	Assert(chunk);

	/* Classify the chunk's parent */
	reltype = classify_inheritance_table(chunk->hypertable_relid, htcache, &ht);

	if (reltype == RELTYPE_COMPRESSION_HYPERTABLE)
		add_chunk_stats(&stats->compression_hypertable, class, chunk, NULL);
	else
	{
		FormData_compression_chunk_size comp_stats_data;
		Form_compression_chunk_size compr_stats = NULL;

		if (get_chunk_compression_stats(statsctx, chunk, &comp_stats_data))
			compr_stats = &comp_stats_data;

		/* Classify the chunk's parent */
		switch (reltype)
		{
			case RELTYPE_HYPERTABLE:
				add_chunk_stats(&stats->hypertables, class, chunk, compr_stats);
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE:
				add_chunk_stats(&stats->distributed_hypertables, class, chunk, compr_stats);
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER:
				add_chunk_stats(&stats->distributed_hypertable_members, class, chunk, compr_stats);
				break;
			case RELTYPE_MATERIALIZED_HYPERTABLE:
				add_chunk_stats(&stats->continuous_aggs, class, chunk, compr_stats);
				break;
			default:
				break;
		}
	}
}

static bool is_information_or_catalog_schema(Oid namespace)
{
	static Oid information_schema_oid = InvalidOid;
	static Oid timescaledb_information_oid = InvalidOid;

	if (namespace == PG_CATALOG_NAMESPACE || namespace == PG_TOAST_NAMESPACE)
		return true;

	if (!OidIsValid(information_schema_oid))
		information_schema_oid = get_namespace_oid("information_schema", false);

	if (!OidIsValid(timescaledb_information_oid))
		timescaledb_information_oid = get_namespace_oid("timescaledb_information", false);

	return namespace == information_schema_oid || namespace == timescaledb_information_oid;
}

static bool
should_ignore_relation(const Catalog *catalog, Form_pg_class class)
{
	return (is_information_or_catalog_schema(class->relnamespace) ||
			isAnyTempNamespace(class->relnamespace) ||
			class->relnamespace == catalog->cache_schema_id ||
			class->relnamespace == catalog->catalog_schema_id ||
			class->relnamespace == catalog->config_schema_id || ts_is_catalog_table(class->oid));
}

static void
stats_init(AllRelkindStats *stats)
{
	MemSet(stats, 0, sizeof(*stats));
}

/*
 * Scan the entire pg_class catalog table for all relations. For each
 * relation, classify it and gather basic stats.
 */
void
ts_telemetry_relation_stats(AllRelkindStats *stats)
{
	const Catalog *catalog = ts_catalog_get();
	Relation rel;
	SysScanDesc scan;
	Cache *htcache = ts_hypertable_cache_pin();
	MemoryContext oldmcxt, relmcxt;
	StatsProcessCtx statsctx = {
		.stats = stats,
		.iterator_valid = false,
	};

	stats_init(stats);
	statsctx.compressed_chunk_stats_iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, AccessShareLock, CurrentMemoryContext);
	ts_scan_iterator_set_index(&statsctx.compressed_chunk_stats_iterator,
							   COMPRESSION_CHUNK_SIZE,
							   COMPRESSION_CHUNK_SIZE_PKEY);

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
		StatsRelType reltype;
		const Chunk *chunk = NULL;
		const Hypertable *ht = NULL;

		MemoryContextReset(relmcxt);
		tup = systable_getnext(scan);

		if (!HeapTupleIsValid(tup))
			break;

		class = (Form_pg_class) GETSTRUCT(tup);

		if (should_ignore_relation(catalog, class))
			continue;

		reltype = classify_relation(class, htcache, &ht, &chunk);

		switch (reltype)
		{
			case RELTYPE_HYPERTABLE:
				process_relation_stats(&stats->hypertables.storage.base, class);
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE:
				process_distributed_hypertable_stats(&stats->distributed_hypertables.storage.base,
													 class);
				break;
			case RELTYPE_REPLICATED_DISTRIBUTED_HYPERTABLE:
				break;
			case RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER:
				process_relation_stats(&stats->distributed_hypertable_members.storage.base, class);
				break;
			case RELTYPE_COMPRESSION_HYPERTABLE:
				process_relation_stats(&stats->compression_hypertable.storage.base, class);
				break;
			case RELTYPE_MATERIALIZED_HYPERTABLE:
				process_relation_stats(&stats->materialized_hypertable.storage.base, class);
				break;
			case RELTYPE_TABLE:
				process_relation_stats(&stats->tables.base, class);
				break;
			case RELTYPE_FOREIGN_TABLE:
				break;
			case RELTYPE_INHERITANCE_TABLE:
				break;
			case RELTYPE_PARTITIONED_TABLE:
				process_relation_stats(&stats->partitioned_tables.base, class);
				break;
			case RELTYPE_CHUNK:
			case RELTYPE_DISTRIBUTED_CHUNK:
				process_chunk_stats(&statsctx, class, chunk, htcache);
				break;
			case RELTYPE_COMPRESSION_CHUNK:
				add_chunk_stats(&stats->compression_hypertable, class, chunk, NULL);
				break;
			case RELTYPE_MATERIALIZED_CHUNK:
				add_chunk_stats(&stats->continuous_aggs, class, chunk, NULL);
				break;
			case RELTYPE_PARTITION:
				break;
			case RELTYPE_INDEX:
				break;
			case RELTYPE_PARTITIONED_INDEX:
				break;
			case RELTYPE_VIEW:
				/* Filter internal cagg views */
				if (class->relnamespace != catalog->internal_schema_id)
					process_relation_stats(&stats->views, class);
				break;
			case RELTYPE_MATVIEW:
				process_relation_stats(&stats->materialized_views.base, class);
				break;
			case RELTYPE_CONTINUOUS_AGG:
				process_relation_stats(&stats->continuous_aggs.storage.base, class);
				break;
			case RELTYPE_OTHER:
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
