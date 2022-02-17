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
#include "chunk_constraint.h"
#include "ts_catalog/chunk_data_node.h"

static void
chunk_constraint_scan_iterator_reset(ScanIterator *it, int32 slice_id)
{
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(
		it,
		Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(slice_id));
}

static ScanIterator
chunk_constraint_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, result_mcxt);
	it.ctx.index = catalog_get_index(ts_catalog_get(),
									 CHUNK_CONSTRAINT,
									 CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

static ScanIterator
chunk_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK, AccessShareLock, result_mcxt);
	it.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

static void
chunk_scan_iterator_reset(ScanIterator *it, int32 chunk_id)
{
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}

static ScanIterator
chunk_data_nodes_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_chunk_data_node_iterator_by_chunk_id(AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

static void
chunk_data_nodes_scan_iterator_reset(ScanIterator *it, int32 chunk_id)
{
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_data_node_chunk_id_node_name_idx_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}

static List *
chunk_scan_stubs(ScanIterator *constr_it, const Hyperspace *hs, const List *dimension_vecs,
				 MemoryContext per_tuple_mcxt)
{
	ListCell *lc;
	List *chunk_stubs = NIL;
	HTAB *htab;
	MemoryContext orig_mcxt;
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	htab = hash_create("chunk-stubs-hash", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	orig_mcxt = MemoryContextSwitchTo(per_tuple_mcxt);

	foreach (lc, dimension_vecs)
	{
		const DimensionVec *vec = lfirst(lc);
		int i;

		for (i = 0; i < vec->num_slices; i++)
		{
			const DimensionSlice *slice = vec->slices[i];
			ChunkStub *stub;

			chunk_constraint_scan_iterator_reset(constr_it, slice->fd.id);

			if (ts_scan_iterator_is_started(constr_it))
				ts_scan_iterator_rescan(constr_it);
			else
				ts_scan_iterator_start_scan(constr_it);

			while (ts_scan_iterator_next(constr_it) != NULL)
			{
				bool isnull, found;
				TupleInfo *ti = ts_scan_iterator_tuple_info(constr_it);
				Datum chunk_id_datum;
				int32 chunk_id;
				ChunkScanEntry *entry;
				MemoryContext old_mcxt;

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

				/* A stub is complete when we've added slices for all its dimensions,
				 * i.e., a complete hypercube */
				if (chunk_stub_is_complete(stub, hs))
				{
					old_mcxt = MemoryContextSwitchTo(orig_mcxt);
					chunk_stubs = lappend(chunk_stubs, stub);
					MemoryContextSwitchTo(old_mcxt);
				}
			}
		}
	}

	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(per_tuple_mcxt);
	hash_destroy(htab);

	return chunk_stubs;
}

/*
 * Lock the chunk if the lockmode tells us to.
 *
 * Also check that the chunk relation actually exists after the lock is
 * acquired. Returns true if the chunk relation exists, otherwise return
 * false.
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

Chunk **
ts_chunk_scan_by_constraints(const Hyperspace *hs, const List *dimension_vecs,
							 LOCKMODE chunk_lockmode, unsigned int *numchunks)
{
	MemoryContext work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "chunk-scan-work", ALLOCSET_DEFAULT_SIZES);
	MemoryContext per_tuple_mcxt =
		AllocSetContextCreate(work_mcxt, "chunk-scan-per-tuple", ALLOCSET_DEFAULT_SIZES);
	MemoryContext orig_mcxt;
	MemoryContext old_mcxt;
	ScanIterator constr_it;
	ScanIterator chunk_it;
	ScanIterator data_node_it;
	ListCell *lc;
	Chunk **chunks;
	unsigned int chunk_count = 0;
	List *chunk_stubs = NIL;
	int remote_chunk_count = 0;
	int i = 0;

	Assert(OidIsValid(hs->main_table_relid));
	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	constr_it = chunk_constraint_scan_iterator_create(orig_mcxt);
	chunk_it = chunk_scan_iterator_create(orig_mcxt);
	data_node_it = chunk_data_nodes_scan_iterator_create(orig_mcxt);
	chunk_stubs = chunk_scan_stubs(&constr_it, hs, dimension_vecs, per_tuple_mcxt);
	chunks = MemoryContextAllocZero(orig_mcxt, sizeof(Chunk *) * list_length(chunk_stubs));
	// MemoryContextSwitchTo(per_tuple_mcxt);

	foreach (lc, chunk_stubs)
	{
		const ChunkStub *stub = lfirst(lc);
		TupleInfo *ti;

		Assert(chunk_stub_is_complete(stub, hs));
		// MemoryContextReset(per_tuple_mcxt);

		chunk_scan_iterator_reset(&chunk_it, stub->id);

		if (ts_scan_iterator_is_started(&chunk_it))
			ts_scan_iterator_rescan(&chunk_it);
		else
			ts_scan_iterator_start_scan(&chunk_it);

		ti = ts_scan_iterator_next(&chunk_it);
		Assert(ti);

		if (ti)
		{
			bool isnull;
			Datum datum = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);
			bool is_dropped = isnull ? false : DatumGetBool(datum);

			if (!is_dropped)
			{
				Chunk *chunk = ts_chunk_build_from_tuple_and_stub(NULL, ti, stub);
				Oid schema_oid;

				/* Fill in table relids. Note that we cannot do this in
				 * chunk_build_from_tuple_and_stub() since chunk_resurrect() also uses
				 * that function and, in that case, the chunk object is needed to create
				 * the data table and related objects. */
				schema_oid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
				chunk->table_id = get_relname_relid(NameStr(chunk->fd.table_name), schema_oid);
				chunk->hypertable_relid = hs->main_table_relid;
				chunk->relkind = get_rel_relkind(chunk->table_id);
				Assert(OidIsValid(chunk->table_id));

				if (lock_chunk_exists(chunk->table_id, chunk_lockmode))
				{
					chunks[chunk_count++] = chunk;

					if (chunk->relkind == RELKIND_FOREIGN_TABLE)
						remote_chunk_count++;
				}
			}

			/* Only one chunk should match */
			Assert(ts_scan_iterator_next(&chunk_it) == NULL);
		}
	}

	Assert(chunk_count <= list_length(chunk_stubs));

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
			chunk_data_nodes_scan_iterator_reset(&data_node_it, chunk->fd.id);

			if (ts_scan_iterator_is_started(&data_node_it))
				ts_scan_iterator_rescan(&data_node_it);
			else
				ts_scan_iterator_start_scan(&data_node_it);

			while (ts_scan_iterator_next(&data_node_it) != NULL)
			{
				bool should_free;
				TupleInfo *ti = ts_scan_iterator_tuple_info(&data_node_it);
				ChunkDataNode *chunk_data_node;
				Form_chunk_data_node form;
				HeapTuple tuple;

				// MemoryContextReset(per_tuple_mcxt);

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

	return chunks;
}

Oid *
ts_chunk_scan_oids_by_constraints(const Hyperspace *hs, const List *dimension_vecs,
								  LOCKMODE chunk_lockmode, unsigned int *numchunks)
{
	MemoryContext work_mcxt =
		AllocSetContextCreate(CurrentMemoryContext, "chunk-scan-work", ALLOCSET_DEFAULT_SIZES);
	MemoryContext per_tuple_mcxt =
		AllocSetContextCreate(work_mcxt, "chunk-scan-per-tuple", ALLOCSET_DEFAULT_SIZES);
	ScanIterator constr_it;
	ScanIterator chunk_it;
	MemoryContext orig_mcxt;
	List *chunk_stubs = NIL;
	ListCell *lc;
	Oid *oids;
	int chunk_count = 0;

	orig_mcxt = MemoryContextSwitchTo(work_mcxt);

	/* Create iterators and initialize scans */
	constr_it = chunk_constraint_scan_iterator_create(orig_mcxt);
	chunk_it = chunk_scan_iterator_create(orig_mcxt);

	/* Scan for matching constraints and return the corresponding chunk stubs */
	chunk_stubs = chunk_scan_stubs(&constr_it, hs, dimension_vecs, per_tuple_mcxt);
	oids = MemoryContextAllocZero(orig_mcxt, sizeof(Oid) * list_length(chunk_stubs));

	// MemoryContextSwitchTo(per_tuple_mcxt);

	foreach (lc, chunk_stubs)
	{
		const ChunkStub *stub = lfirst(lc);
		TupleInfo *ti;

		Assert(chunk_stub_is_complete(stub, hs));
		// MemoryContextReset(per_tuple_mcxt);
		chunk_scan_iterator_reset(&chunk_it, stub->id);

		if (ts_scan_iterator_is_started(&chunk_it))
			ts_scan_iterator_rescan(&chunk_it);
		else
			ts_scan_iterator_start_scan(&chunk_it);

		ti = ts_scan_iterator_next(&chunk_it);
		Assert(ti);

		if (ti)
		{
			bool isnull;
			Datum datum = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);
			bool is_dropped = isnull ? false : DatumGetBool(datum);

			if (!is_dropped)
			{
				Datum table_name, schema_name;
				Oid chunk_oid = InvalidOid;
				Oid schema_oid;

				table_name = slot_getattr(ti->slot, Anum_chunk_table_name, &isnull);
				Assert(!isnull);
				schema_name = slot_getattr(ti->slot, Anum_chunk_schema_name, &isnull);
				Assert(!isnull);

				schema_oid = get_namespace_oid(NameStr(*DatumGetName(schema_name)), false);
				chunk_oid = get_relname_relid(NameStr(*DatumGetName(table_name)), schema_oid);
				Assert(OidIsValid(chunk_oid));

				if (lock_chunk_exists(chunk_oid, chunk_lockmode))
					oids[chunk_count++] = chunk_oid;
			}

			/* Should only be one chunk that matches */
			Assert(ts_scan_iterator_next(&chunk_it) == NULL);
		}
	}

	ts_scan_iterator_close(&chunk_it);
	ts_scan_iterator_close(&constr_it);
	MemoryContextSwitchTo(orig_mcxt);
	MemoryContextDelete(work_mcxt);

	if (numchunks)
		*numchunks = chunk_count;

	return oids;
}
