/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Cache for the invalidations read by the plugin.
 *
 * The cache is used to accumulate invalidations for continuous aggregates based on 
 * changes to the source hypertable aggregated. When processing the WAL, 
 * multiple entries in the WAL might invalidate the same or overlapping ranges. The 
 * cache is used to merge those ranges into singular ranges until the cache is flushed
 * at the end of a transaction.
 *
 * The cache key is the source hypertable relid from which the 
 * invalidation was generated.
 */

#include <postgres.h>
#include <fmgr.h>

#include "invalidation_plugin_cache.h"

#include <inttypes.h>

/*
 * Record the value in the invalidation range cache.
 */
void
invalidation_cache_write_record(HTAB *cache, Oid relid, int64 value)
{
	bool found;
	InvalidationCacheEntry *entry = hash_search(cache, &relid, HASH_ENTER, &found);
	if (!found)
		entry->hypertable_relid = relid;
	if (!found || value < entry->lowest_modified_value)
		entry->lowest_modified_value = value;
	if (!found || value > entry->greatest_modified_value)
		entry->greatest_modified_value = value;
}

HTAB *
invalidation_cache_create(MemoryContext mcxt)
{
	HASHCTL ctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(InvalidationCacheEntry),
		.hcxt = mcxt,
	};

	return hash_create("hypertable log invalidation cache",
					   32,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
invalidation_cache_destroy(HTAB *cache)
{
	hash_destroy(cache);
}

void
invalidation_cache_foreach_record(HTAB *cache, ProcessInvalidationFunction func,
								  InvalidationsContext *args)
{
	HASH_SEQ_STATUS hash_seq;
	InvalidationCacheEntry *entry;
	int count = hash_get_num_entries(cache);

	hash_seq_init(&hash_seq, cache);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		(*func)(entry, (--count == 0), args);
}
