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
	BufferHeapTupleTableSlot base;
	TupleTableSlot *compressed_slot;
	ArrowArray **arrow_columns;
	uint16 tuple_index; /* Index of this particular tuple in the compressed (columnar data) tuple */
	MemoryContext decompression_mcxt;
	Bitmapset *segmentby_columns;
	char *data;
} ArrowTupleTableSlot;

extern const TupleTableSlotOps TTSOpsArrowTuple;

extern TupleTableSlot *ExecStoreArrowTuple(TupleTableSlot *slot, TupleTableSlot *compressed_slot,
										   uint16 tuple_index);
extern TupleTableSlot *ExecStoreArrowTupleExisting(TupleTableSlot *slot, uint16 tuple_index);
extern void tts_arrow_set_heaptuple_mode(TupleTableSlot *slot);

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#define MaxCompressedBlockNumber ((BlockNumber) 0x3FFFFF)

static inline void
tid_to_compressed_tid(ItemPointer out_tid, ItemPointer in_tid, uint16 tuple_index)
{
	BlockNumber blockno = ItemPointerGetBlockNumber(in_tid);
	OffsetNumber offsetno = ItemPointerGetOffsetNumber(in_tid);
	BlockNumber index_blockno = (blockno << 10) | (tuple_index & 0x3FF);

	Assert(blockno <= MaxCompressedBlockNumber);
	ItemPointerSet(out_tid, index_blockno, offsetno);
}

static inline uint16
compressed_tid_to_tid(ItemPointer out_tid, ItemPointer in_tid)
{
	BlockNumber blockno = ItemPointerGetBlockNumber(in_tid);
	OffsetNumber offsetno = ItemPointerGetOffsetNumber(in_tid);
	uint16 tuple_index = (blockno & 0x3FF);
	BlockNumber orig_blockno = (blockno >> 10);

	ItemPointerSet(out_tid, orig_blockno, offsetno);

	return tuple_index;
}

#endif /* PG_ARROW_TUPTABLE_H */
