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

#define TTS_IS_ARROWTUPLE(slot) ((slot)->tts_ops == &TTSOpsArrowTuple)

#endif /* PG_ARROW_TUPTABLE_H */
