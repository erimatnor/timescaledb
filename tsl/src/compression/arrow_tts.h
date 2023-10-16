/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef PG_ARROW_TUPTABLE_H
#define PG_ARROW_TUPTABLE_H

#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>

#include "arrow_c_data_interface.h"

typedef struct ArrowTupleTableSlot
{
	VirtualTupleTableSlot base;
	TupleTableSlot *child_slot;
	ArrowArray **arrow_columns;
	uint16 tuple_index; /* Index of this particular tuple in the compressed
						 * (columnar data) child tuple. Note that the first
						 * value has index 1. If the index is 0 it means the
						 * child slot points to a non-compressed tuple. */
	MemoryContext decompression_mcxt;
	Bitmapset *segmentby_columns;
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, TupleTableSlot *child_slot,
										   uint16 tuple_index);
extern TupleTableSlot *ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#define InvalidTupleIndex 0
#define MaxCompressedBlockNumber ((BlockNumber) 0x3FFFFF)

static inline void
tid_to_compressed_tid(ItemPointer out_tid, ItemPointer in_tid, uint16 tuple_index)
{
	BlockNumber blockno = ItemPointerGetBlockNumber(in_tid);
	OffsetNumber offsetno = ItemPointerGetOffsetNumber(in_tid);
	BlockNumber compressed_blockno;

	Assert(blockno <= MaxCompressedBlockNumber);

	compressed_blockno = ((tuple_index & 0x3FF) << 22) | blockno;
	ItemPointerSet(out_tid, compressed_blockno, offsetno);
}

static inline uint16
compressed_tid_to_tid(ItemPointer out_tid, ItemPointer in_tid)
{
	BlockNumber blockno = ItemPointerGetBlockNumber(in_tid);
	OffsetNumber offsetno = ItemPointerGetOffsetNumber(in_tid);
	uint16 tuple_index = (blockno >> 22);
	BlockNumber orig_blockno = (blockno & 0x3FFFFF);

	ItemPointerSet(out_tid, orig_blockno, offsetno);

	return tuple_index;
}

#define is_compressed_tid(itemptr) ((ItemPointerGetBlockNumber(itemptr) >> 22) != 0)

#endif /* PG_ARROW_TUPTABLE_H */
