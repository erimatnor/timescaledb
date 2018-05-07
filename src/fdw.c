#include <postgres.h>
#include <foreign/fdwapi.h>
#include <fmgr.h>

#include "compat.h"

static void
fdw_get_relsize(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{

}

static void
fdw_get_paths(PlannerInfo *root,
			  RelOptInfo *baserel,
			  Oid foreigntableid)
{

}

static ForeignScan *
fdw_get_plan(PlannerInfo *root,
			 RelOptInfo *baserel,
			 Oid foreigntableid,
			 ForeignPath *best_path,
			 List *tlist,
			 List *scan_clauses,
			 Plan *outer_plan)
{

	return NULL;
}

static void
fdw_begin_scan(ForeignScanState *node,
					   int eflags)
{

}

static TupleTableSlot *
fdw_iterate_scan(ForeignScanState *node)
{

	return NULL;
}

/*
static bool
fdw_recheck_scan(ForeignScanState *node,
						 TupleTableSlot *slot)
{

}
*/

static void
fdw_rescan(ForeignScanState *node)
{

}

static void
fdw_end_scan(ForeignScanState *node)
{

}

TS_FUNCTION_INFO_V1(timescaledb_fdw_handler);

Datum
timescaledb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = fdw_get_relsize;
	routine->GetForeignPaths = fdw_get_paths;
	routine->GetForeignPlan = fdw_get_plan;
	routine->BeginForeignScan = fdw_begin_scan;
	routine->IterateForeignScan = fdw_iterate_scan;
	routine->ReScanForeignScan = fdw_rescan;
	routine->EndForeignScan = fdw_end_scan;

	PG_RETURN_POINTER(routine);
}
