#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

typedef struct ChunkDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti;
	Oid hypertable_relid;
} ChunkDispatchPath;

extern Path *chunk_dispatch_path_create(ModifyTablePath *mtpath, Path *subpath, Index hypertable_rti, Oid hypertable_relid);
extern CustomScan *chunk_dispatch_plan_create_old(ModifyTable *mt, Plan *subplan, Index hypertable_rti, Oid hypertable_relid);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
