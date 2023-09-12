#include <postgres.h>

#include <access/attnum.h>
#include <executor/tuptable.h>
#include <utils/expandeddatum.h>

#include "utils/palloc.h"
#include "arrow_tts.h"
#include "compression.h"
#include "custom_type_cache.h"

static void
tts_arrow_init(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	aslot->arrow_columns = NULL;
	aslot->compressed_slot = NULL;
	aslot->segmentby_columns = NULL;
	aslot->decompression_mcxt = AllocSetContextCreate(slot->tts_mcxt,
													  "bulk decompression",
													  /* minContextSize = */ 0,
													  /* initBlockSize = */ 64 * 1024,
													  /* maxBlockSize = */ 64 * 1024);
}

static void
tts_arrow_release(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	if (NULL != aslot->arrow_columns)
	{
		aslot->arrow_columns = NULL;
	}
}

static void
tts_arrow_clear(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		/* The tuple is materialized, so free materialized memory */
	}

	if (aslot->arrow_columns)
	{
		for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			ArrowArray *arr = aslot->arrow_columns[i];

			if (arr)
			{
				pfree(arr);
			}

			aslot->arrow_columns[i] = NULL;
		}

		pfree(aslot->arrow_columns);
		aslot->arrow_columns = NULL;
	}

	aslot->compressed_slot = NULL;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static inline void
tts_arrow_store_tuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot, uint16 tuple_index)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	aslot->compressed_slot = compressed_slot;
	aslot->tuple_index = tuple_index;
	ItemPointerCopy(&compressed_slot->tts_tid, &slot->tts_tid);
	slot->tts_tid.ip_blkid.bi_hi = tuple_index;
	Assert(!TTS_EMPTY(aslot->compressed_slot));
}

TupleTableSlot *
ExecStoreArrowTuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot, uint16 tuple_index)
{
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	Assert(!TTS_EMPTY(compressed_slot));

	if (unlikely(!TTS_IS_ARROWTUPLE(slot)))
		elog(ERROR, "trying to store an on-disk parquet tuple into wrong type of slot");

	ExecClearTuple(slot);
	tts_arrow_store_tuple(slot, compressed_slot, tuple_index);

	Assert(!TTS_EMPTY(slot));
	Assert(!TTS_SHOULDFREE(slot));

	return slot;
}

TupleTableSlot *
ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));
	aslot->tuple_index = tuple_index;
	slot->tts_tid.ip_blkid.bi_hi = aslot->tuple_index;
	slot->tts_nvalid = 0;

	return slot;
}

/*
 * Read the slot's values from the Parquet file and store its
 * tts_values[] array.
 */
static void
tts_arrow_materialize(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
	TupleDesc desc = slot->tts_tupleDescriptor;
	Size sz = 0;
	char *data;

	Assert(!TTS_EMPTY(slot));

	/* If slot has its tuple already materialized, nothing to do. */
	if (TTS_SHOULDFREE(slot))
	{
		elog(NOTICE, "tuple already materialized");
		return;
	}

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* Mark all entries in the tts_values array as valid */
	slot->tts_nvalid = desc->natts;

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	aslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* TODO: This code is intended to copy data from Arrow record (or
	 * some other storage format) to heap tuple format that is used by
	 * PostgreSQL.
	 *
	 * This do not work currently because varlen attributes are not
	 * stored in the expected format. (They are actually not
	 * translated at all currently.)
	 *
	 * We need to update this code once we decide how to pass back
	 * varlen attributes from the Arrow record. */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		elog(WARNING, "trying to materialize varlen data for column %s", NameStr(att->attname));

		if (att->attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static bool
is_compressed_col(const TupleDesc tupdesc, AttrNumber attno)
{
	static CustomTypeInfo *typinfo = NULL;
	Oid coltypid = tupdesc->attrs[AttrNumberGetAttrOffset(attno)].atttypid;

	if (typinfo == NULL)
		typinfo = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA);

	return coltypid == typinfo->type_oid;
}

static void
tts_arrow_getsomeattrs(TupleTableSlot *slot, int natts)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	if (natts < 1 || natts > slot->tts_tupleDescriptor->natts)
		elog(ERROR, "invalid number of attributes requested");

	slot_getsomeattrs(aslot->compressed_slot, natts);

	if (NULL == aslot->arrow_columns)
	{
		aslot->arrow_columns =
			MemoryContextAlloc(slot->tts_mcxt,
							   sizeof(ArrowArray *) * slot->tts_tupleDescriptor->natts);

		for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			aslot->arrow_columns[i] = NULL;
		}
	}

	for (int i = 0; i < natts; i++)
	{
		const AttrNumber attno = AttrOffsetGetAttrNumber(i);
		const TupleDesc compressed_tupdesc = aslot->compressed_slot->tts_tupleDescriptor;

		/* Decompress the column if not already done. */
		if (aslot->arrow_columns[i] == NULL)
		{
			if (is_compressed_col(compressed_tupdesc, attno))
			{
				bool isnull;
				Datum value = slot_getattr(aslot->compressed_slot, attno, &isnull);

				if (isnull)
				{
					// do nothing
				}
				else
				{
					const CompressedDataHeader *header =
						(CompressedDataHeader *) PG_DETOAST_DATUM(value);
					DecompressAllFunction decompress_all =
						tsl_get_decompress_all_function(header->compression_algorithm);
					Assert(decompress_all != NULL);
					MemoryContext oldcxt = MemoryContextSwitchTo(aslot->decompression_mcxt);
					aslot->arrow_columns[i] =
						decompress_all(PointerGetDatum(header),
									   slot->tts_tupleDescriptor->attrs[i].atttypid,
									   slot->tts_mcxt);
					MemoryContextReset(aslot->decompression_mcxt);
					MemoryContextSwitchTo(oldcxt);
				}
			}
			else
			{
				/* Since we are looping over the attributes of the
				 * non-compressed slot, we will either see only compressed
				 * columns or the segment-by column. If the column is not
				 * compressed, it must be the segment-by columns. The
				 * segment-by column is not compressed and the value is the
				 * same for all rows in the compressed tuple. */
				aslot->arrow_columns[i] = NULL;
				slot->tts_values[i] =
					slot_getattr(aslot->compressed_slot, attno, &slot->tts_isnull[i]);

				/* Remember the segment-by column */
				MemoryContext oldcxt = MemoryContextSwitchTo(slot->tts_mcxt);
				aslot->segmentby_columns = bms_add_member(aslot->segmentby_columns, attno);
				MemoryContextSwitchTo(oldcxt);
			}
		}

		/* At this point the column should be decompressed, if it is a
		 * compressed column. */
		if (bms_is_member(attno, aslot->segmentby_columns))
		{
			/* Segment-by column. Value already set. */
		}
		else if (aslot->arrow_columns[i] == NULL)
		{
			/* Since the column is not the segment-by column, and there is no
			 * decompressed data, the column must be NULL. Use the default
			 * value. */
			slot->tts_values[i] = getmissingattr(slot->tts_tupleDescriptor,
												 AttrOffsetGetAttrNumber(i),
												 &slot->tts_isnull[i]);
		}
		else
		{
			const char *restrict values = aslot->arrow_columns[i]->buffers[1];
			const uint64 *restrict validity = aslot->arrow_columns[i]->buffers[0];
			int16 value_bytes = get_typlen(slot->tts_tupleDescriptor->attrs[i].atttypid);

			/*
			 * The conversion of Datum to more narrow types will truncate
			 * the higher bytes, so we don't care if we read some garbage
			 * into them, and can always read 8 bytes. These are unaligned
			 * reads, so technically we have to do memcpy.
			 */
			uint64 value;
			memcpy(&value, &values[value_bytes * aslot->tuple_index], 8);

#ifdef USE_FLOAT8_BYVAL
			Datum datum = Int64GetDatum(value);
#else
			/*
			 * On 32-bit systems, the data larger than 4 bytes go by
			 * reference, so we have to jump through these hoops.
			 */
			Datum datum;
			if (column_values.value_bytes <= 4)
			{
				datum = Int32GetDatum((uint32) value);
			}
			else
			{
				datum = Int64GetDatum(value);
			}
#endif
			slot->tts_values[i] = datum;
			slot->tts_isnull[i] = !arrow_row_is_valid(validity, aslot->tuple_index);
		}
	}

	slot->tts_nvalid = natts;
}

/*
 */
static Datum
tts_arrow_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot retrieve a system column in this context")));

	return 0; /* silence compiler warnings */
}

static void
tts_arrow_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	// ArrowTupleTableSlot *asrcslot = (ArrowTupleTableSlot *)srcslot;
	// ArrowTupleTableSlot *adstslot = (ArrowTupleTableSlot *)dstslot;
	MemoryContext oldcontext;

	tts_arrow_clear(dstslot);
	slot_getallattrs(srcslot);

	oldcontext = MemoryContextSwitchTo(dstslot->tts_mcxt);
	// adstslot->record = arrow_record_hold(asrcslot->record);
	MemoryContextSwitchTo(oldcontext);
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_arrow_materialize(dstslot);
}

static HeapTuple
tts_arrow_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple tuple;

	Assert(!TTS_EMPTY(slot));

	tts_arrow_materialize(slot);
	tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
	ItemPointerCopy(&slot->tts_tid, &tuple->t_self);

	return tuple;
}

static MinimalTuple
tts_arrow_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));
	tts_arrow_materialize(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
}

const TupleTableSlotOps TTSOpsArrowTuple = { .base_slot_size = sizeof(ArrowTupleTableSlot),
											 .init = tts_arrow_init,
											 .release = tts_arrow_release,
											 .clear = tts_arrow_clear,
											 .getsomeattrs = tts_arrow_getsomeattrs,
											 .getsysattr = tts_arrow_getsysattr,
											 .materialize = tts_arrow_materialize,
											 .copyslot = tts_arrow_copyslot,
											 .get_heap_tuple = NULL,
											 .get_minimal_tuple = NULL,
											 .copy_heap_tuple = tts_arrow_copy_heap_tuple,
											 .copy_minimal_tuple = tts_arrow_copy_minimal_tuple };
