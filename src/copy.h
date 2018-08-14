#ifndef TIMESCALEDB_COPY_H
#define TIMESCALEDB_COPY_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <access/xact.h>
#include <executor/executor.h>
#include <commands/copy.h>

typedef struct Hypertable Hypertable;

typedef struct CopyChunkState CopyChunkState;

typedef bool (*CopyFromFunc) (CopyChunkState *state, ExprContext *econtext, Datum *values, bool *nulls, Oid *tuple_oid);

void		timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed, Hypertable *ht);
int64		timescaledb_copy_to(Hypertable *ht, CopyFromFunc next_tuple_func, void *priv, LOCKMODE lockmode);
int64		timescaledb_move_from_table_to_chunks(Hypertable *ht, LOCKMODE lockmode);
void	   *copy_chunk_state_private_data(CopyChunkState *state);

#endif							/* TIMESCALEDB_COPY_H */
