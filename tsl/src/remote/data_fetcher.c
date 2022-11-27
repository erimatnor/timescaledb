/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "data_fetcher.h"
#include "cursor_fetcher.h"
#include "copy_fetcher.h"
#include "guc.h"
#include "errors.h"

#define DEFAULT_FETCH_SIZE 100

void
data_fetcher_init(DataFetcher *df, TSConnection *conn, const char *stmt, StmtParams *params,
				  TupleFactory *tf)
{
	Assert(df != NULL);
	Assert(stmt != NULL);

	memset(df, 0, sizeof(DataFetcher));
	df->tuples = NULL;
	df->conn = conn;
	df->stmt = pstrdup(stmt);
	df->stmt_params = params;
	df->tf = tf;
	df->state = DF_INIT;

	tuplefactory_set_per_tuple_mctx_reset(df->tf, false);
	df->batch_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "cursor tuple data", ALLOCSET_DEFAULT_SIZES);
	df->tuple_mctx = df->batch_mctx;
	df->req_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "async req/resp", ALLOCSET_DEFAULT_SIZES);
	df->fetch_size = DEFAULT_FETCH_SIZE;
}

static const char *statenames[] = {
	[DF_INIT] = "INIT",
	[DF_OPEN] = "OPEN",
	[DF_FILE_TRAILER_RECEIVED] = "FILE TRAILER",
	[DF_EOF] = "EOF",
	[DF_CLOSED] = "CLOSED",
};

void
data_fetcher_transition(DataFetcher *df, DataFetcherState new_state)
{
	Assert((df->state == DF_INIT && new_state == DF_OPEN) ||
		   (df->state == DF_EOF && new_state == DF_CLOSED) ||
		   (df->state == DF_FILE_TRAILER_RECEIVED && (new_state == DF_EOF || new_state == DF_CLOSED)) ||
		   (df->state == DF_OPEN && (new_state == DF_FILE_TRAILER_RECEIVED || new_state == DF_EOF || new_state == DF_CLOSED)));

	elog(LOG, "[%s]: DF transitioning from %s to %s",
		 remote_connection_node_name(df->conn), statenames[df->state], statenames[new_state]);
	df->state = new_state;
}

void
data_fetcher_validate(DataFetcher *df)
{
	/* ANALYZE command is accessing random tuples so we should never fail here when running ANALYZE
	 */
	if (df->next_tuple_idx != 0 && df->next_tuple_idx < df->num_tuples)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR),
				 errmsg("invalid cursor state. sql: %s", df->stmt),
				 errhint("Shouldn't fetch new data before consuming existing.")));
}

void
data_fetcher_store_tuple(DataFetcher *df, int row, TupleTableSlot *slot)
{
	if (row >= df->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (df->state == DF_EOF || df->funcs->fetch_data(df) == 0)
		{
			ExecClearTuple(slot);
			return;
		}

		/* More data was fetched so need to reset row index */
		row = 0;
		Assert(row == df->next_tuple_idx);
	}

	Assert(df->tuples != NULL);
	Assert(row >= 0 && row < df->num_tuples);

	/*
	 * Return the next tuple. Must force the tuple into the slot since
	 * CustomScan initializes ss_ScanTupleSlot to a VirtualTupleTableSlot
	 * while we're storing a HeapTuple.
	 */
	ExecForceStoreHeapTuple(df->tuples[row], slot, /* shouldFree = */ false);
}

void
data_fetcher_store_next_tuple(DataFetcher *df, TupleTableSlot *slot)
{
	data_fetcher_store_tuple(df, df->next_tuple_idx, slot);

	if (!TupIsNull(slot))
		df->next_tuple_idx++;

	Assert(df->next_tuple_idx <= df->num_tuples);
}

void
data_fetcher_set_fetch_size(DataFetcher *df, int fetch_size)
{
	df->fetch_size = fetch_size;
}

void
data_fetcher_set_tuple_mctx(DataFetcher *df, MemoryContext mctx)
{
	Assert(mctx != NULL);
	df->tuple_mctx = mctx;
}

void
data_fetcher_reset(DataFetcher *df)
{
	df->tuples = NULL;
	df->num_tuples = 0;
	df->next_tuple_idx = 0;
	df->batch_count = 0;
	df->state = DF_INIT;
	MemoryContextReset(df->req_mctx);
	MemoryContextReset(df->batch_mctx);
}

void
data_fetcher_free(DataFetcher *df)
{
	df->funcs->close(df);
	pfree(df);
}
