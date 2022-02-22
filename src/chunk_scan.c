/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <storage/lmgr.h>
#include <utils/syscache.h>

#include "dimension_vector.h"
#include "hypertable.h"
#include "hypercube.h"
#include "scan_iterator.h"
#include "chunk_scan.h"
#include "chunk.h"
#include "chunk_cache.h"
#include "chunk_constraint.h"
#include "ts_catalog/chunk_data_node.h"

/*
 * Find the chunks that match a query.
 *
 * The input is a set of dimension vectors that contain the dimension slices
 * that match a query. Each dimension vector contains all matching dimension
 * slices in one particular dimension.
 *
 * The output is a list of chunks (in the form of partial chunk stubs) whose
 * complete set of dimension slices exist in the given dimension vectors. In
 * other words, we only care about the chunks that match in all dimensions.
 */
static List *
scan_stubs_by_constraints(ScanIterator *constr_it, const Hyperspace *hs, const List *dimension_vecs,
						  MemoryContext per_tuple_mcxt)
{
	ListCell *lc;
	List *complete_chunk_stubs = NIL;
	HTAB *htab;
	MemoryContext orig_mcxt = CurrentMemoryContext;
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	htab = hash_create("chunk-stubs-hash", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	/*
	 * Scan for chunk constraints that reference the slices in the dimension
	 * vectors. Collect the chunk constraints in a hash table keyed on chunk
	 * ID. After the scan, there will be some chunk IDs in the hash table that
	 * have a complete set of constraints (one for each dimension). These are
	 * the chunks that match the query.
	 */
	foreach (lc, dimension_vecs)
	{
		const DimensionVec *vec = lfirst(lc);
		int i;

		for (i = 0; i < vec->num_slices; i++)
		{
			const DimensionSlice *slice = vec->slices[i];
			ChunkStub *stub;

			ts_chunk_constraint_scan_iterator_set_slice_id(constr_it, slice->fd.id);
			ts_scan_iterator_start_or_restart_scan(constr_it);

			while (ts_scan_iterator_next(constr_it) != NULL)
			{
				bool isnull, found;
				TupleInfo *ti = ts_scan_iterator_tuple_info(constr_it);
				Datum chunk_id_datum;
				int32 chunk_id;
				ChunkScanEntry *entry;
				MemoryContext old_mcxt;

				MemoryContextSwitchTo(per_tuple_mcxt);
				MemoryContextReset(per_tuple_mcxt);

				chunk_id_datum = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull);
				Assert(!isnull);
				chunk_id = DatumGetInt32(chunk_id_datum);

				if (slot_attisnull(ts_scan_iterator_slot(constr_it),
								   Anum_chunk_constraint_dimension_slice_id))
					continue;

				entry = hash_search(htab, &chunk_id, HASH_ENTER, &found);
				old_mcxt = MemoryContextSwitchTo(ti->mctx);

				if (!found)
				{
					stub = ts_chunk_stub_create(chunk_id, hs->num_dimensions);
					stub->cube = ts_hypercube_alloc(hs->num_dimensions);
					entry->stub = stub;
				}
				else
					stub = entry->stub;

				ts_chunk_constraints_add_from_tuple(stub->constraints, ti);
				ts_hypercube_add_slice(stub->cube, slice);
				MemoryContextSwitchTo(old_mcxt);

				/* A stub is complete when we've added constraints for all its
				 * dimensions */
				if (chunk_stub_is_complete(stub, hs))
				{
					old_mcxt = MemoryContextSwitchTo(orig_mcxt);
					complete_chunk_stubs = lappend(complete_chunk_stubs, stub);
					MemoryContextSwitchTo(old_mcxt);
					/* The hypercube should also be complete */
					Assert(stub->cube->num_slices == hs->num_dimensions);
					/* Slices should be in dimension ID order */
					ts_hypercube_slice_sort(stub->cube);
				}

				MemoryContextSwitchTo(orig_mcxt);
			}
		}
	}

	hash_destroy(htab);

	return complete_chunk_stubs;
}

/*
 * Lock the chunk if the lockmode demands it.
 *
 * Also check that the chunk relation actually exists after the lock is
 * acquired. Return true if the chunk relation exists, otherwise return false.
 */
static bool
lock_chunk_exists(Oid chunk_oid, LOCKMODE chunk_lockmode)
{
	/* No lock is requests, so assume relation exists */
	if (chunk_lockmode == NoLock)
		return true;

	/* Get the lock to synchronize against concurrent drop */
	LockRelationOid(chunk_oid, chunk_lockmode);

	/*
	 * Now that we have the lock, double-check to see if the relation
	 * really exists or not.  If not, assume it was dropped while we
	 * waited to acquire lock, and ignore it.
	 */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(chunk_oid)))
	{
		/* Release useless lock */
		UnlockRelationOid(chunk_oid, chunk_lockmode);
		/* And ignore this relation */
		return false;
	}

	return true;
}

/*
 * Scan for chunks matching a query.
 *
 */
Chunk **
ts_chunk_scan_by_constraints(const Hyperspace *hs, const List *dimension_vecs,
							 LOCKMODE chunk_lockmode, unsigned int *numchunks)
{
	MemoryContext work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "chunk-scan-work", ALLOCSET_DEFAULT_SIZES);
	MemoryContext per_tuple_mcxt =
		AllocSetContextCreate(work_mcxt, "chunk-scan-per-tuple", ALLOCSET_SMALL_SIZES);
	MemoryContext orig_mcxt;
	ScanIterator constr_it;
	ScanIterator chunk_it;
	ScanIterator data_node_it;
	ListCell *lc;
	Chunk **chunks = NULL;
	unsigned int chunk_count = 0;
	List *chunk_stubs = NIL;
	int remote_chunk_count = 0;
	int i = 0;

	Assert(OidIsValid(hs->main_table_relid));
	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	constr_it = ts_chunk_constraint_scan_iterator_create(orig_mcxt);
	chunk_stubs = scan_stubs_by_constraints(&constr_it, hs, dimension_vecs, per_tuple_mcxt);

	if (list_length(chunk_stubs) == 0)
	{
		ts_scan_iterator_close(&constr_it);
		MemoryContextSwitchTo(orig_mcxt);
		MemoryContextDelete(work_mcxt);

		if (numchunks)
			*numchunks = 0;

		return NULL;
	}

	chunk_it = ts_chunk_scan_iterator_create(orig_mcxt);
	data_node_it = ts_chunk_data_nodes_scan_iterator_create(orig_mcxt);
	Cache *ccache = ts_chunk_cache_pin();

	foreach (lc, chunk_stubs)
	{
		const ChunkStub *stub = lfirst(lc);
		TupleInfo *ti;
		Chunk *chunk;

		Assert(CurrentMemoryContext == work_mcxt);
		Assert(chunk_stub_is_complete(stub, hs));

		chunk = ts_chunk_cache_get_entry_by_id(ccache,
											   stub->id,
											   CACHE_FLAG_NOCREATE | CACHE_FLAG_MISSING_OK);

		if (NULL == chunk)
		{
			ts_chunk_scan_iterator_set_chunk_id(&chunk_it, stub->id);
			ts_scan_iterator_start_or_restart_scan(&chunk_it);
			ti = ts_scan_iterator_next(&chunk_it);

			if (ti)
			{
				bool isnull;
				Datum datum = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);
				bool is_dropped = isnull ? false : DatumGetBool(datum);

				MemoryContextSwitchTo(per_tuple_mcxt);
				MemoryContextReset(per_tuple_mcxt);

				if (!is_dropped)
				{
					int num_constraints_hint = stub->constraints->num_constraints;
					MemoryContext old_mcxt;
					Oid schema_oid;

					chunk = MemoryContextAllocZero(ti->mctx, sizeof(Chunk));
					ts_chunk_formdata_fill(&chunk->fd, ti);

					/*
					 * The chunk stub scan only gave us dimensional
					 * constraints. Scan again for all constraints.
					 */
					chunk->constraints = ts_chunk_constraints_alloc(num_constraints_hint, ti->mctx);

					MemoryContextSwitchTo(work_mcxt);
					ts_chunk_constraint_scan_iterator_set_chunk_id(&constr_it, chunk->fd.id);
					ts_scan_iterator_rescan(&constr_it);

					while (ts_scan_iterator_next(&constr_it) != NULL)
					{
						TupleInfo *constr_ti = ts_scan_iterator_tuple_info(&constr_it);
						MemoryContextSwitchTo(per_tuple_mcxt);
						ts_chunk_constraints_add_from_tuple(chunk->constraints, constr_ti);
						MemoryContextSwitchTo(work_mcxt);
					}

					MemoryContextSwitchTo(per_tuple_mcxt);

					/* Copy the hypercube into the result memory context */
					old_mcxt = MemoryContextSwitchTo(ti->mctx);
					chunk->cube = ts_hypercube_copy(stub->cube);
					MemoryContextSwitchTo(old_mcxt);

					/* Fill in table relids. Note that we cannot do this in
					 * chunk_build_from_tuple_and_stub() since chunk_resurrect() also uses
					 * that function and, in that case, the chunk object is needed to create
					 * the data table and related objects. */
					schema_oid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
					chunk->table_id = get_relname_relid(NameStr(chunk->fd.table_name), schema_oid);
					chunk->hypertable_relid = hs->main_table_relid;
					chunk->relkind = get_rel_relkind(chunk->table_id);
					Assert(OidIsValid(chunk->table_id));
				}

				/* Only one chunk should match */
				Assert(ts_scan_iterator_next(&chunk_it) == NULL);
				MemoryContextSwitchTo(work_mcxt);
			}
		}
		else
		{
			MemoryContext old_mcxt = MemoryContextSwitchTo(orig_mcxt);
			chunk = ts_chunk_copy(chunk);
			// elog(NOTICE, "Got chunk %d from cache", chunk->fd.id);
			MemoryContextSwitchTo(old_mcxt);
		}

		if (chunk != NULL && lock_chunk_exists(chunk->table_id, chunk_lockmode))
		{
			/* Lazy initialize the chunks array */
			if (NULL == chunks)
				chunks =
					MemoryContextAllocZero(orig_mcxt, sizeof(Chunk *) * list_length(chunk_stubs));

			chunks[chunk_count] = chunk;
			ts_chunk_cache_put_entry(ccache, chunk, true);

			if (chunk->relkind == RELKIND_FOREIGN_TABLE)
				remote_chunk_count++;

			chunk_count++;
		}
	}

	ts_cache_release(ccache);

	Assert(chunks == NULL || chunk_count > 0);
	Assert(chunk_count <= list_length(chunk_stubs));
	Assert(CurrentMemoryContext == work_mcxt);

	/*
	 * Fill in data nodes for remote chunks.
	 *
	 * Typically, either all chunks are remote chunks or none are.
	 */
	for (i = 0; remote_chunk_count > 0 && i < chunk_count; i++)
	{
		Chunk *chunk = chunks[i];

		if (chunk->relkind == RELKIND_FOREIGN_TABLE)
		{
			/* Must start or restart the scan on the longer-lived context */
			ts_chunk_data_nodes_scan_iterator_set_chunk_id(&data_node_it, chunk->fd.id);
			ts_scan_iterator_start_or_restart_scan(&data_node_it);

			while (ts_scan_iterator_next(&data_node_it) != NULL)
			{
				bool should_free;
				TupleInfo *ti = ts_scan_iterator_tuple_info(&data_node_it);
				ChunkDataNode *chunk_data_node;
				Form_chunk_data_node form;
				MemoryContext old_mcxt;
				HeapTuple tuple;

				MemoryContextSwitchTo(per_tuple_mcxt);
				MemoryContextReset(per_tuple_mcxt);

				tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
				form = (Form_chunk_data_node) GETSTRUCT(tuple);
				old_mcxt = MemoryContextSwitchTo(ti->mctx);
				chunk_data_node = palloc(sizeof(ChunkDataNode));
				memcpy(&chunk_data_node->fd, form, sizeof(FormData_chunk_data_node));
				chunk_data_node->foreign_server_oid =
					get_foreign_server_oid(NameStr(form->node_name),
										   /* missing_ok = */ false);
				chunk->data_nodes = lappend(chunk->data_nodes, chunk_data_node);
				MemoryContextSwitchTo(old_mcxt);

				if (should_free)
					heap_freetuple(tuple);

				MemoryContextSwitchTo(work_mcxt);
			}
		}
	}

	ts_scan_iterator_close(&data_node_it);
	ts_scan_iterator_close(&chunk_it);
	ts_scan_iterator_close(&constr_it);

	if (numchunks)
		*numchunks = chunk_count;

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

#ifdef USE_ASSERT_CHECKING
	/* Assert that we always return valid chunks */
	for (i = 0; i < chunk_count; i++)
	{
		ASSERT_IS_VALID_CHUNK(chunks[i]);
	}
#endif

	return chunks;
}
