/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/stratnum.h>
#include <access/tableam.h>
#include <catalog/dependency.h>
#include <catalog/objectaddress.h>
#include <nodes/parsenodes.h>
#include <storage/itemptr.h>
#include <storage/lockdefs.h>

#include "chunk_rewrite.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"

static HeapTuple
chunk_rewrite_make_tuple(Oid chunk_relid, Oid new_relid, TupleDesc desc)
{
	Datum values[Natts_chunk_rewrite];
	bool nulls[Natts_chunk_rewrite] = { false };

	memset(values, 0, sizeof(Datum) * Natts_chunk_rewrite);

	values[AttrNumberGetAttrOffset(Anum_chunk_rewrite_chunk_relid)] = ObjectIdGetDatum(chunk_relid);
	values[AttrNumberGetAttrOffset(Anum_chunk_rewrite_new_relid)] = ObjectIdGetDatum(new_relid);

	return heap_form_tuple(desc, values, nulls);
}

void
ts_chunk_rewrite_add(Oid chunk_relid, Oid new_relid)
{
	Catalog *catalog = ts_catalog_get();
	Oid cat_relid = catalog_get_table_id(catalog, CHUNK_REWRITE);
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;
	Relation catrel;

	catrel = table_open(cat_relid, RowExclusiveLock);
	new_tuple = chunk_rewrite_make_tuple(chunk_relid, new_relid, catrel->rd_att);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_only(catrel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	table_close(catrel, NoLock);
}

bool
ts_chunk_rewrite_get_with_lock(Oid chunk_relid, Form_chunk_rewrite form, ItemPointer tid)
{
	Catalog *catalog = ts_catalog_get();
	ScanIterator it;
	ScanTupLock tuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	bool found = false;

	it = ts_scan_iterator_create(CHUNK_REWRITE, RowShareLock, CurrentMemoryContext);
	it.ctx.tuplock = &tuplock;
	it.ctx.flags = SCANNER_F_KEEPLOCK;
	it.ctx.index = catalog_get_index(catalog, CHUNK_REWRITE, CHUNK_REWRITE_IDX);
	ts_scan_iterator_scan_key_init(&it,
								   Anum_chunk_rewrite_key_chunk_relid,
								   BTEqualStrategyNumber,
								   F_OIDEQ,
								   ObjectIdGetDatum(chunk_relid));

	ts_scanner_foreach(&it)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);

		switch (ti->lockresult)
		{
			case TM_Ok:
				found = true;

				if (tid)
					ItemPointerCopy(&ti->slot->tts_tid, tid);

				if (form)
				{
					bool should_free;
					HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
					memcpy(form, GETSTRUCT(tuple), sizeof(FormData_chunk_rewrite));

					if (should_free)
						heap_freetuple(tuple);
				}
				break;
			case TM_Deleted:
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("chunk merge state deleted by concurrent transaction")));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to lock chunk rewrite catalog tuple, lock result is %d for "
								"chunk (%u)",
								ti->lockresult,
								chunk_relid)));
				break;
		}
	}

	ts_scan_iterator_close(&it);

	return found;
}

void
ts_chunk_rewrite_delete_by_tid(const ItemPointer tid)
{
	Catalog *catalog = ts_catalog_get();
	Oid cat_relid = catalog_get_table_id(catalog, CHUNK_REWRITE);
	CatalogSecurityContext sec_ctx;
	Relation catrel;

	catrel = table_open(cat_relid, RowExclusiveLock);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid_only(catrel, tid);
	ts_catalog_restore_user(&sec_ctx);
	table_close(catrel, NoLock);
}

bool
ts_chunk_rewrite_delete(Oid chunk_relid)
{
	ItemPointerData tid;
	FormData_chunk_rewrite form;

	if (!ts_chunk_rewrite_get_with_lock(chunk_relid, &form, &tid))
		return false;

	/*
	 * Check if the new heap still exists by trying to get a lock.
	 */
	Relation newrel = try_table_open(form.new_relid, AccessExclusiveLock);

	if (newrel)
	{
		ObjectAddress tableaddr;
		/* New heap still exists, so delete it */
		table_close(newrel, NoLock);
		ObjectAddressSet(tableaddr, RelationRelationId, form.new_relid);
		performDeletion(&tableaddr, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
	}

	ts_chunk_rewrite_delete_by_tid(&tid);

	return true;
}
