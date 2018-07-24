#ifndef TIMESCALEDB_HYPERTABLE_INSERT_H
#define TIMESCALEDB_HYPERTABLE_INSERT_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct HypertableInsertPath
{
	CustomPath cpath;
	List *servers;
} HypertableInsertPath;

typedef struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableInsertState;

extern Path    *hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath);
extern Plan	   *hypertable_insert_plan_create_2(Hypertable *ht, ModifyTable *mt);

#endif							/* TIMESCALEDB_HYPERTABLE_INSERT_H */
