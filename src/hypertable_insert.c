#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <executor/nodeModifyTable.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <catalog/pg_type.h>

#include "hypertable_insert.h"
#include "chunk_dispatch_info.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_server.h"
#include "hypertable_cache.h"

/*
 * HypertableInsert (with corresponding executor node) is a plan node that
 * implements INSERTs for hypertables. It is mostly a wrapper around the
 * ModifyTable plan node that simply calls the wrapped ModifyTable plan without
 * doing much else, apart from some initial state setup.
 *
 * The wrapping is needed to setup state in the execution phase, and give access
 * to the ModifyTableState node to sub-plan states in the PlanState tree. For
 * instance, the ChunkDispatchState node needs to set the arbiter index list in
 * the ModifyTableState node whenever it inserts into a new chunk.
 */

static void
hypertable_insert_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableInsertState *state = (HypertableInsertState *) node;
	ResultRelInfo *hypertable_result_relinfo;
	PlanState  *ps;

	hypertable_result_relinfo = estate->es_result_relations + state->mt->resultRelIndex;

	ps = ExecInitNode(&state->mt->plan, estate, eflags);

	node->custom_ps = list_make1(ps);

	if (IsA(ps, ModifyTableState))
	{
		ModifyTableState *mtstate = (ModifyTableState *) ps;
		int			i;

		/*
		 * Find all ChunkDispatchState subnodes and set their parent
		 * ModifyTableState node
		 */
		for (i = 0; i < mtstate->mt_nplans; i++)
		{
			if (IsA(mtstate->mt_plans[i], CustomScanState))
			{
				CustomScanState *csstate = (CustomScanState *) mtstate->mt_plans[i];

				if (strcmp(csstate->methods->CustomName, CHUNK_DISPATCH_STATE_NAME) == 0)
				{
					ChunkDispatchState *cdstate = (ChunkDispatchState *) mtstate->mt_plans[i];

					chunk_dispatch_state_set_parent(cdstate, mtstate);
				}
			}
		}
	}
}

static TupleTableSlot *
hypertable_insert_exec(CustomScanState *node)
{
	return ExecProcNode(linitial(node->custom_ps));
}

static void
hypertable_insert_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
hypertable_insert_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static CustomExecMethods hypertable_insert_state_methods = {
	.CustomName = "HypertableInsertState",
	.BeginCustomScan = hypertable_insert_begin,
	.EndCustomScan = hypertable_insert_end,
	.ExecCustomScan = hypertable_insert_exec,
	.ReScanCustomScan = hypertable_insert_rescan,
};

static Node *
hypertable_insert_state_create(CustomScan *cscan)
{
	HypertableInsertState *state;

	state = (HypertableInsertState *) newNode(sizeof(HypertableInsertState), T_CustomScanState);
	state->cscan_state.methods = &hypertable_insert_state_methods;
	state->mt = (ModifyTable *) cscan->scan.plan.lefttree;

	return (Node *) state;
}

static CustomScanMethods hypertable_insert_plan_methods = {
	.CustomName = "HypertableInsert",
	.CreateCustomScanState = hypertable_insert_state_create,
};

Plan *
hypertable_insert_plan_create_2(Hypertable *ht, ModifyTable *mt)
{
	CustomScan *cscan = makeNode(CustomScan);
	ListCell *lc;

	cscan->methods = &hypertable_insert_plan_methods;
	cscan->custom_plans = list_make1(mt);
	cscan->scan.plan.lefttree = &mt->plan;
	cscan->scan.scanrelid = 0;	/* This is not a real relation */

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;
	cscan->scan.plan.targetlist = mt->plan.targetlist;
	cscan->custom_scan_tlist = NIL;

	foreach(lc, ht->servers)
	{
		HypertableServer *server = lfirst(lc);
		cscan->custom_private = lappend_oid(cscan->custom_private, server->foreign_server_oid);
	}

	if (list_length(mt->fdwPrivLists) > 0)
	{
		elog(NOTICE, "Hypertable ModifyTable plan has private FDW data. list length %d", list_length(mt->fdwPrivLists));
	} else {
		elog(NOTICE, "ModifyTable has no private FDW data");
	}

	return &cscan->scan.plan;
}

static Plan *
hypertable_insert_plan_create(PlannerInfo *root,
							  RelOptInfo *rel,
							  struct CustomPath *best_path,
							  List *tlist,
							  List *clauses,
							  List *custom_plans)
{

	CustomScan *cscan = makeNode(CustomScan);
	//ListCell *lc;
	ModifyTable *mt = linitial(custom_plans);

	elog(NOTICE, "Creating hypertable insert plan");
	Assert(IsA(mt, ModifyTable));

	cscan->methods = &hypertable_insert_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.plan.lefttree = &mt->plan;
	cscan->scan.scanrelid = 0;	/* This is not a real relation */

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;
	cscan->scan.plan.targetlist = tlist; //mt->plan.targetlist;
	cscan->custom_scan_tlist = NIL;

	/*
	foreach(lc, ht->servers)
	{
		HypertableServer *server = lfirst(lc);
		cscan->custom_private = lappend_oid(cscan->custom_private, server->foreign_server_oid);
	}

	if (list_length(mt->fdwPrivLists) > 0)
	{
		elog(NOTICE, "Hypertable ModifyTable plan has private FDW data. list length %d", list_length(mt->fdwPrivLists));
	} else {
		elog(NOTICE, "ModifyTable has no private FDW data");
		} */

	return &cscan->scan.plan;
}

static CustomPathMethods hypertable_insert_path_methods = {
	.CustomName = "HypertableInsertPath",
	.PlanCustomPath = hypertable_insert_plan_create,
};

Path *
hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath)
{
	Path *path = &mtpath->path;
	Cache *hcache = hypertable_cache_pin();
	ListCell *lc_path, *lc_rel;
	List *subpaths = NIL;
	Hypertable *ht = NULL;

	forboth(lc_path, mtpath->subpaths, lc_rel, mtpath->resultRelations)
	{
		Path *subpath = lfirst(lc_path);
		Index		rti = lfirst_int(lc_rel);
		RangeTblEntry *rte = planner_rt_fetch(rti, root);

		elog(NOTICE, "INSERT PATH: found RTE %s", get_rel_name(rte->relid));

		ht = hypertable_cache_get_entry(hcache, rte->relid);

		if (ht != NULL)
		{
			if (root->parse->onConflict != NULL &&
				root->parse->onConflict->constraint != InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertables do not support ON CONFLICT statements that reference constraints"),
						 errhint("Use column names to infer indexes instead.")));

			/*
			 * We replace the plan with our custom chunk dispatch
			 * plan.
			 */
			subpaths = lappend(subpaths, chunk_dispatch_path_create(mtpath, subpath, rti, rte->relid));
			//*subpath = chunk_dispatch_path_create(mtpath, *subpath, rti, rte->relid);
		}
	   else
		   subpaths = lappend(subpaths, subpath);
	}

	if (NULL != ht)
	{
		HypertableInsertPath *hipath = (HypertableInsertPath *) palloc0(sizeof(HypertableInsertPath));

		memcpy(&hipath->cpath.path, path, sizeof(Path));
		hipath->cpath.path.type = T_CustomPath;
		hipath->cpath.path.pathtype = T_CustomScan;
		hipath->cpath.custom_paths = list_make1(mtpath);
		hipath->cpath.methods = &hypertable_insert_path_methods;
		mtpath->subpaths = subpaths;
		path = &hipath->cpath.path;
		elog(NOTICE, "DoONE");
	}

	cache_release(hcache);

	return path;
}
