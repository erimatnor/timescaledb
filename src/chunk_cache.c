/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>

#include "errors.h"
#include "chunk_cache.h"
#include "ts_catalog/catalog.h"
#include "cache.h"
#include "chunk.h"
#include "ts_catalog/tablespace.h"

static void *chunk_cache_create_entry(Cache *cache, CacheQuery *query);
static void chunk_cache_missing_error(const Cache *cache, const CacheQuery *query);

typedef struct ChunkCache
{
	Cache base;
	HTAB *idmap; /* Hash table to map chunk_id to Oid */
} ChunkCache;

typedef struct ChunkIdEntry
{
	int32 chunk_id;
	Chunk *chunk;
} ChunkIdEntry;

typedef struct ChunkCacheQuery
{
	CacheQuery q;
	Oid relid;
	const char *schema;
	const char *table;
	Chunk *chunk;
} ChunkCacheQuery;

static void *
chunk_cache_get_key(CacheQuery *query)
{
	return &((ChunkCacheQuery *) query)->relid;
}

typedef struct
{
	Oid relid;
	Chunk *chunk;
} ChunkCacheEntry;

static bool
chunk_cache_valid_result(const void *result)
{
	if (result == NULL)
		return false;
	return ((ChunkCacheEntry *) result)->chunk != NULL;
}

static ChunkCache *
chunk_cache_create()
{
	MemoryContext ctx =
		AllocSetContextCreate(CacheMemoryContext, "Chunk cache", ALLOCSET_DEFAULT_SIZES);
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkIdEntry),
		.hcxt = ctx,
	};
	ChunkCache *cache = MemoryContextAlloc(ctx, sizeof(ChunkCache));

	cache->base = (Cache)
	{
		.hctl =
		{
			.keysize = sizeof(Oid),
			.entrysize = sizeof(ChunkCacheEntry),
			.hcxt = ctx,
		},
		.name = "chunk_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = chunk_cache_get_key,
		.create_entry = chunk_cache_create_entry,
		.missing_error = chunk_cache_missing_error,
		.valid_result = chunk_cache_valid_result,
	};

	cache->idmap =
		hash_create("chunk-stubs-hash", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ts_cache_init(&cache->base);

	return cache;
}

static ChunkCache *chunk_cache_current = NULL;

static void *
chunk_cache_create_entry(Cache *cache, CacheQuery *query)
{
	ChunkCache *ccache = (ChunkCache *) cache;
	ChunkCacheQuery *hq = (ChunkCacheQuery *) query;
	ChunkCacheEntry *cache_entry = query->result;
	ChunkIdEntry *id_entry;
	bool found;

	if (NULL != hq->chunk)
	{
		cache_entry->chunk = hq->chunk;
	}
	else
	{
		if (NULL == hq->schema)
			hq->schema = get_namespace_name(get_rel_namespace(hq->relid));

		if (NULL == hq->table)
			hq->table = get_rel_name(hq->relid);

		cache_entry->chunk = ts_chunk_get_by_name_with_memory_context(hq->schema,
																	  hq->table,
																	  ts_cache_memory_ctx(cache),
																	  false);
	}

	if (NULL != cache_entry->chunk)
	{
		id_entry = hash_search(ccache->idmap, &cache_entry->chunk->fd.id, HASH_ENTER, &found);
		Assert(!found);
		id_entry->chunk = cache_entry->chunk;
	}

	return cache_entry->chunk == NULL ? NULL : cache_entry;
}

static void
chunk_cache_missing_error(const Cache *cache, const CacheQuery *query)
{
	ChunkCacheQuery *hq = (ChunkCacheQuery *) query;

	if (OidIsValid(hq->relid))
	{
		const char *const rel_name = get_rel_name(hq->relid);

		if (rel_name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("OID %u does not refer to a chunk table", hq->relid)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_CHUNK_NOT_EXIST),
					 errmsg("table \"%s\" is not a chunk", rel_name)));
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("chunk does not exist")));
	}
}

void
ts_chunk_cache_invalidate_callback(void)
{
	ts_cache_invalidate(&chunk_cache_current->base);
	chunk_cache_current = chunk_cache_create();
}

void
ts_chunk_cache_put_entry(Cache *cache, Chunk *chunk, bool copy)
{
	ChunkCacheQuery query = {
		.relid = chunk->table_id,
		.chunk = NULL,
	};

	if (copy)
	{
		MemoryContext old_mcxt = MemoryContextSwitchTo(ts_cache_memory_ctx(cache));
		chunk = ts_chunk_copy(chunk);
		MemoryContextSwitchTo(old_mcxt);
	}

	query.chunk = chunk;
	ts_cache_fetch(cache, &query.q);
}

/* Get hypertable cache entry. If the entry is not in the cache, add it. */
Chunk *
ts_chunk_cache_get_entry(Cache *const cache, const Oid relid, const unsigned int flags)
{
	if (!OidIsValid(relid))
	{
		if (flags & CACHE_FLAG_MISSING_OK)
			return NULL;
		else
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid Oid")));
	}

	return ts_chunk_cache_get_entry_with_table(cache, relid, NULL, NULL, flags);
}

/*
 * Returns cache into the argument and hypertable as the function result.
 * If hypertable is not found, fails with an error.
 */
Chunk *
ts_chunk_cache_get_cache_and_entry(const Oid relid, const unsigned int flags, Cache **const cache)
{
	*cache = ts_chunk_cache_pin();
	return ts_chunk_cache_get_entry(*cache, relid, flags);
}

Chunk *
ts_chunk_cache_get_entry_rv(Cache *cache, const RangeVar *rv)
{
	return ts_chunk_cache_get_entry(cache, RangeVarGetRelid(rv, NoLock, true), true);
}

TSDLLEXPORT Chunk *
ts_chunk_cache_get_entry_by_id(Cache *cache, const int32 chunk_id, const unsigned flags)
{
	ChunkCache *ccache = (ChunkCache *) cache;
	ChunkIdEntry *id_entry;
	bool found;
	Chunk *chunk;
	MemoryContext old_mcxt;

	id_entry = hash_search(ccache->idmap, &chunk_id, HASH_FIND, &found);

	if (found)
	{
		Assert(id_entry->chunk);
		return id_entry->chunk;
	}

	if (flags & CACHE_FLAG_NOCREATE)
	{
		if (flags & CACHE_FLAG_MISSING_OK)
			return NULL;

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("chunk with ID %d does not exist", chunk_id)));
	}

	old_mcxt = MemoryContextSwitchTo(ts_cache_memory_ctx(cache));
	chunk = ts_chunk_get_by_id(chunk_id, false);
	MemoryContextSwitchTo(old_mcxt);

	if (NULL != chunk)
	{
		ChunkCacheQuery query = {
			.relid = chunk->table_id,
			.q.flags = flags,
			.chunk = chunk,
		};
		ChunkCacheEntry *entry = ts_cache_fetch(cache, &query.q);
		Assert((flags & CACHE_FLAG_MISSING_OK) ? true : (entry != NULL && entry->chunk != NULL));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("chunk with ID %d does not exist", chunk_id)));
	}

	return chunk;
}

Chunk *
ts_chunk_cache_get_entry_with_table(Cache *cache, const Oid relid, const char *schema,
									const char *table, const unsigned int flags)
{
	ChunkCacheQuery query = {
		.q.flags = flags,
		.relid = relid,
		.schema = schema,
		.table = table,
	};
	ChunkCacheEntry *entry = ts_cache_fetch(cache, &query.q);
	Assert((flags & CACHE_FLAG_MISSING_OK) ? true : (entry != NULL && entry->chunk != NULL));
	return entry == NULL ? NULL : entry->chunk;
}

extern TSDLLEXPORT Cache *
ts_chunk_cache_pin()
{
	return ts_cache_pin(&chunk_cache_current->base);
}

void
_chunk_cache_init(void)
{
	CreateCacheMemoryContext();
	chunk_cache_current = chunk_cache_create();
}

void
_chunk_cache_fini(void)
{
	ts_cache_invalidate(&chunk_cache_current->base);
}
