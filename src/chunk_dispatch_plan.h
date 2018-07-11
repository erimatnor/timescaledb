#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

extern CustomScan *chunk_dispatch_plan_create(ModifyTable *mt, Plan *subplan, Index hypertable_rti, Oid hypertable_relid);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
