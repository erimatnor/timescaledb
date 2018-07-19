#ifndef TIMESCALEDB_HYPERTABLE_INSERT_H
#define TIMESCALEDB_HYPERTABLE_INSERT_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableInsertState;

Plan	   *hypertable_insert_plan_create(Hypertable *ht, ModifyTable *mt);

#endif							/* TIMESCALEDB_HYPERTABLE_INSERT_H */
