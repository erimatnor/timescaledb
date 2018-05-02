#ifndef TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H
#define TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H

#include <postgres.h>
#include <nodes/relation.h>
#include <nodes/extensible.h>

typedef struct ConstraintAwareAppendInfo ConstraintAwareAppendInfo;

typedef struct ConstraintAwareAppendPath
{
	CustomPath	cpath;
	ConstraintAwareAppendInfo *info;
} ConstraintAwareAppendPath;

typedef struct ConstraintAwareAppendState
{
	CustomScanState csstate;
	Plan	   *subplan;
	Size		num_append_subplans;
} ConstraintAwareAppendState;

typedef struct Hypertable Hypertable;

Path	   *constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath,
												bool perform_exclusion);

void _constraint_aware_append_init(void);
void _constraint_aware_append_fini(void);

#endif							/* TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H */
