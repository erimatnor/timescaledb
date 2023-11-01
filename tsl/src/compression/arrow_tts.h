/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef PG_ARROW_TUPTABLE_H
#define PG_ARROW_TUPTABLE_H

#include <postgres.h>
#include <access/tupdesc.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>

#include "arrow_c_data_interface.h"
#include <utils/palloc.h>

#include <limits.h>

typedef struct ArrowTupleTableSlot
{
	VirtualTupleTableSlot base;
	TupleTableSlot *child_slot;
	TupleTableSlot *noncompressed_slot;
	TupleTableSlot *compressed_slot;
	TupleDesc compressed_tupdesc;
	ArrowArray **arrow_columns;
	uint16 tuple_index; /* Index of this particular tuple in the compressed
						 * (columnar data) child tuple. Note that the first
						 * value has index 1. If the index is 0 it means the
						 * child slot points to a non-compressed tuple. */
	MemoryContext decompression_mcxt;
	Bitmapset *segmentby_columns;
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, uint16 tuple_index);
extern TupleTableSlot *ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#define InvalidTupleIndex 0
#define MaxCompressedBlockNumber ((BlockNumber) 0x3FFFFF)

/*
 * The compressed TID is encoded in the following manner, which places a limit
 * on 1024 rows in a single compressed tuple. Since we are currently storing
 * 1000 rows that should work.
 *
 *         32 bits                16 bits
 * +-------------------------+-----------------+
 * |       Block Number      |  Offset Number  |
 * +------+------------------+---+-------------+
 * | Flag | Compressed Tuple TID | Tuple Index |
 * +------+----------------------+-------------+
 *  1 bit         33 bits            10 bits
 */

#define BLOCKID_BITS (CHAR_BIT * sizeof(BlockIdData))
#define COMPRESSED_FLAG (1UL << (BLOCKID_BITS - 1))
#define OFFSET_BITS (CHAR_BIT * sizeof(OffsetNumber))
#define OFFSET_MASK (((uint64) 1UL << OFFSET_BITS) - 1)
#define TUPINDEX_BITS (10U)
#define TUPINDEX_MASK (((uint64) 1UL << TUPINDEX_BITS) - 1)

/* Compute a 64-bits value from the item pointer data */
static uint64
bits_from_tid(ItemPointer tid)
{
	return (ItemPointerGetBlockNumber(tid) << OFFSET_BITS) | ItemPointerGetOffsetNumber(tid);
}

/*
 * The "compressed TID" consists of the bits of the TID for the compressed row
 * shifted to insert the tuple index as the least significant bits of the TID.
 */
static inline void
tid_to_compressed_tid(ItemPointer out_tid, ItemPointer in_tid, uint16 tuple_index)
{
	const uint64 bits = (bits_from_tid(in_tid) << TUPINDEX_BITS) | tuple_index;

	Assert(tuple_index != InvalidTupleIndex);

	/* Insert the tuple index as the least significant bits and set the most
	 * significant bit of the block id to mark it as a compressed tuple. */
	const BlockNumber blockno = COMPRESSED_FLAG | (bits >> OFFSET_BITS);
	const OffsetNumber offsetno = bits & OFFSET_MASK;
	ItemPointerSet(out_tid, blockno, offsetno);
}

static inline uint16
compressed_tid_to_tid(ItemPointer out_tid, ItemPointer in_tid)
{
	const uint64 orig_bits = bits_from_tid(in_tid);
	const uint16 tuple_index = orig_bits & TUPINDEX_MASK;

	/* Remove the tuple index bits and clear the compressed flag from the block id. */
	const uint64 bits = orig_bits >> TUPINDEX_BITS;
	BlockNumber blockno = ~COMPRESSED_FLAG & (bits >> OFFSET_BITS);
	OffsetNumber offsetno = bits & OFFSET_MASK;

	Assert(tuple_index != InvalidTupleIndex);

	ItemPointerSet(out_tid, blockno, offsetno);

	return tuple_index;
}

static inline bool
is_compressed_tid(ItemPointer itemptr)
{
	return (ItemPointerGetBlockNumber(itemptr) & COMPRESSED_FLAG) != 0;
}

static inline TupleTableSlot *
arrow_slot_get_compressed_slot(TupleTableSlot *slot, const TupleDesc tupdesc)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));

	if (NULL == aslot->compressed_slot)
	{
		MemoryContext oldmctx;

		if (NULL == tupdesc)
			elog(ERROR, "cannot make compressed table slot without tuple descriptor");

		oldmctx = MemoryContextSwitchTo(slot->tts_mcxt);
		aslot->compressed_slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsBufferHeapTuple);
		MemoryContextSwitchTo(oldmctx);
	}

	return aslot->compressed_slot;
}

static inline TupleTableSlot *
arrow_slot_get_noncompressed_slot(TupleTableSlot *slot)
{
	ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;

	Assert(TTS_IS_ARROWTUPLE(slot));
	Assert(aslot->noncompressed_slot);

	return aslot->noncompressed_slot;
}

#endif /* PG_ARROW_TUPTABLE_H */
