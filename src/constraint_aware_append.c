#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <nodes/makefuncs.h>
#include <nodes/extensible.h>
#include <nodes/readfuncs.h>
#include <parser/parsetree.h>
#include <optimizer/plancat.h>
#include <optimizer/clauses.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <optimizer/pathnode.h>
#include <optimizer/cost.h>
#include <executor/executor.h>
#include <catalog/pg_class.h>
#include <utils/memutils.h>
#include <utils/lsyscache.h>
#include <commands/explain.h>

#include "constraint_aware_append.h"
#include "hypertable.h"
#include "compat.h"
#include "chunk.h"
#include "hypercube.h"
#include "chunk_cache.h"

#define CUSTOM_NAME "ConstraintAwareAppend"

#define strtobool(x)  ((*(x) == 't') ? true : false)

/*
 * Info passed from planner to executor.
 *
 * Must be a planner node that can be copied, so we use ExtensibleNode.
 */
typedef struct ConstraintAwareAppendInfo
{
	ExtensibleNode enode;
	Oid hypertable_relid;
	bool do_exclusion;
	List *append_rel_list;
	List *clauses;
} ConstraintAwareAppendInfo;

static void
constraint_aware_append_info_copy(struct ExtensibleNode *newnode,
								  const struct ExtensibleNode *oldnode)
{
	ConstraintAwareAppendInfo *newinfo = (ConstraintAwareAppendInfo *) newnode;
	const ConstraintAwareAppendInfo *oldinfo = (const ConstraintAwareAppendInfo *) oldnode;

	newinfo->append_rel_list = list_copy(oldinfo->append_rel_list);
	newinfo->clauses = list_copy(oldinfo->clauses);
}

static bool
constraint_aware_append_info_equal(const struct ExtensibleNode *an,
								   const struct ExtensibleNode *bn)
{
	const ConstraintAwareAppendInfo *a = (const ConstraintAwareAppendInfo *) an;
	const ConstraintAwareAppendInfo *b = (const ConstraintAwareAppendInfo *) bn;

	return equal(a->append_rel_list, b->append_rel_list) && equal(a->clauses, b->clauses);
}

static void
constraint_aware_append_info_out(struct StringInfoData *str,
								 const struct ExtensibleNode *node)
{
	const ConstraintAwareAppendInfo *info = (const ConstraintAwareAppendInfo *) node;

	appendStringInfo(str, " :hypertable_relid %u", info->hypertable_relid);
	appendStringInfo(str, " :do_exclusion %c", info->do_exclusion ? 't' : 'f');
	appendStringInfo(str, " :append_rel_list ");
	outNode(str, info->append_rel_list);
	appendStringInfo(str, " :clauses ");
	outNode(str, info->clauses);
}

static void
constraint_aware_append_info_read(struct ExtensibleNode *node)
{
	ConstraintAwareAppendInfo *info = (ConstraintAwareAppendInfo *) node;
	int			length;
	char	   *token;

	/* Skip :hypertable_relid */
	token = pg_strtok(&length);

	info->hypertable_relid = strtol(token, NULL, 10);

	/* Skip :do_exclusion */
	token = pg_strtok(&length);

	info->do_exclusion = strtobool(token);

	/* Skip :append_rel_list */
	token = pg_strtok(&length);

	/* Read the append_rel_list */
	info->append_rel_list = nodeRead(NULL, 0);

	/* Skip :clauses */
	token = pg_strtok(&length);

	/* Read the clauses */
	info->clauses = nodeRead(NULL, 0);
}

static ExtensibleNodeMethods constraint_aware_append_info_methods = {
	.extnodename = "ConstraintAwareAppendInfo",
	.node_size = sizeof(ConstraintAwareAppendInfo),
	.nodeCopy = constraint_aware_append_info_copy,
	.nodeEqual = constraint_aware_append_info_equal,
	.nodeOut = constraint_aware_append_info_out,
	.nodeRead = constraint_aware_append_info_read,
};

static ConstraintAwareAppendInfo *
constraint_aware_append_info_create()
{
	return (ConstraintAwareAppendInfo *) newNode(sizeof(ConstraintAwareAppendInfo), T_ExtensibleNode);
}

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * This functions tries to reuse as much functionality as possible from standard
 * constraint exclusion in PostgreSQL that normally happens at planning
 * time. Therefore, we need to fake a number of planning-related data
 * structures.
 *
 * We also need to walk the expression trees of the restriction clauses and
 * update any Vars that reference the main table to instead reference the child
 * table (chunk) we want to exclude.
 */
static bool
excluded_by_constraint(RangeTblEntry *rte, AppendRelInfo *appinfo, List *restrictinfos)
{
	ListCell   *lc;
	RelOptInfo	rel = {
		.relid = appinfo->child_relid,
		.reloptkind = RELOPT_OTHER_MEMBER_REL,
		.baserestrictinfo = NIL,
	};
	Query		parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

	foreach(lc, restrictinfos)
	{
		/*
		 * We need a copy to retain the original parent ID in Vars for next
		 * chunk
		 */
		RestrictInfo *old = lfirst(lc);
		RestrictInfo *rinfo = makeNode(RestrictInfo);

		/*
		 * Update Vars to reference the child relation (chunk) instead of the
		 * hypertable main table
		 */
		rinfo->clause = (Expr *) adjust_appendrel_attrs(&root, (Node *) old->clause, appinfo);
		rel.baserestrictinfo = lappend(rel.baserestrictinfo, rinfo);
	}

	return relation_excluded_by_constraints(&root, &rel, rte);
}

/*
 * Convert restriction clauses to constants expressions (i.e., if there are
 * mutable functions, they need to be evaluated to constants).  For instance,
 * something like:
 *
 * ...WHERE time > now - interval '1 hour'
 *
 * becomes
 *
 * ...WHERE time > '2017-06-02 11:26:43.935712+02'
 */
static List *
constify_restrictinfos(List *restrictinfos)
{
	List	   *newinfos = NIL;
	ListCell   *lc;
	Query		parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

	foreach(lc, restrictinfos)
	{
		/* We need a copy to not mess up the plan */
		RestrictInfo *old = lfirst(lc);
		RestrictInfo *rinfo = makeNode(RestrictInfo);

		rinfo->clause = (Expr *) estimate_expression_value(&root, (Node *) old->clause);
		newinfos = lappend(newinfos, rinfo);
	}

	return newinfos;
}


static inline bool
is_scan(Plan *plan)
{
	switch (nodeTag(plan))
	{
	case T_SeqScan:
	case T_SampleScan:
	case T_IndexScan:
	case T_IndexOnlyScan:
	case T_BitmapIndexScan:
	case T_BitmapHeapScan:
	case T_TidScan:
	case T_SubqueryScan:
	case T_FunctionScan:
	case T_ValuesScan:
	case T_CteScan:
	case T_WorkTableScan:
	case T_ForeignScan:
	case T_CustomScan:
		return true;
	default:
		return false;
	}
}

static bool
should_exclude_scan(Scan *scan, AppendRelInfo *appinfo, List *restrictinfos, EState *estate)
{
	RangeTblEntry *rte = rt_fetch(scan->scanrelid, estate->es_range_table);

	/*
	 * If this is a base rel (chunk), check if it can be excluded
	 * from the scan. Otherwise, fall through.
	 */
	if (rte->rtekind == RTE_RELATION &&
		rte->relkind == RELKIND_RELATION &&
		!rte->inh &&
		excluded_by_constraint(rte, appinfo, restrictinfos))
		return true;

	return false;
}

static int
exclude_scans(ConstraintAwareAppendState *state,
			  Plan *plan,
			  EState *estate)
{
	int num_scans = 0;
	CustomScan *cscan = (CustomScan *) state->csstate.ss.ps.plan;
	ConstraintAwareAppendInfo *info = linitial(cscan->custom_private);
	List	   *append_rel_list = info->append_rel_list;
	List	   *restrictinfos = constify_restrictinfos(info->clauses);
	List	  **appendplans,
			   *old_appendplans;
	ListCell   *lc_plan,
			   *lc_info;

	switch (nodeTag(plan))
	{
		case T_Append:
			{
				Append	   *append = (Append *) plan;

				old_appendplans = append->appendplans;
				append->appendplans = NIL;
				appendplans = &append->appendplans;
				break;
			}
		case T_MergeAppend:
			{
				MergeAppend *append = (MergeAppend *) plan;

				old_appendplans = append->mergeplans;
				append->mergeplans = NIL;
				appendplans = &append->mergeplans;
				break;
			}
		case T_Result:

			/*
			 * Append plans are turned into a Result node if empty. This can
			 * happen if children are pruned first by constraint exclusion
			 * while we also remove the main table from the appendplans list,
			 * leaving an empty list. In that case, there is nothing to do.
			 */
			return num_scans;
		default:
			elog(ERROR, "invalid plan %d", nodeTag(plan));
	}

	lc_info = list_head(append_rel_list);

	foreach(lc_plan, old_appendplans)
	{
		Plan       *subplan = lfirst(lc_plan);
		AppendRelInfo *appinfo = lfirst(lc_info);

		if (is_scan(subplan))
		{
			Scan *scan = (Scan *) subplan;

			if (!info->do_exclusion ||
				!should_exclude_scan(scan, appinfo, restrictinfos, estate))
			{
				*appendplans = lappend(*appendplans, subplan);
				num_scans++;
			}

			Assert(scan->scanrelid == appinfo->child_relid);
			lc_info = lnext(lc_info);
		}
		/* In case we inserted a MergeAppend, due to overlapping append plans,
		 * we need to descend down to the children scan nodes (chunks) */
		else if (nodeTag(subplan) == T_MergeAppend)
		{
			MergeAppend *ma = (MergeAppend *) subplan;
			List *mergeplans = ma->mergeplans;
			ListCell *lc;

			ma->mergeplans = NIL;

			foreach (lc, mergeplans)
			{
				Plan *ma_plan = lfirst(lc);
				AppendRelInfo *appinfo = lfirst(lc_info);

				if (is_scan(ma_plan))
				{
					Scan *scan = (Scan *) ma_plan;

					if (!info->do_exclusion ||
						!should_exclude_scan(scan, appinfo, restrictinfos, estate))
					{
						ma->mergeplans = lappend(ma->mergeplans, ma_plan);
						num_scans++;
					}

					Assert(scan->scanrelid == appinfo->child_relid);
					lc_info = lnext(lc_info);
				}
			}

			if (list_length(ma->mergeplans) > 0)
				*appendplans = lappend(*appendplans, ma);
		}
	}

	return num_scans;
}

/*
 * Initialize the scan state and prune any subplans from the Append node below
 * us in the plan tree. Pruning happens by evaluating the subplan's table
 * constraints against a folded version of the restriction clauses in the query.
 */
static void
ca_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	Plan	   *subplan = copyObject(state->subplan);

	state->num_append_subplans = exclude_scans(state, subplan, estate);

	if (state->num_append_subplans > 0)
		node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

static TupleTableSlot *
ca_append_exec(CustomScanState *node)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	TupleTableSlot *subslot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
#if PG96
	TupleTableSlot *resultslot;
	ExprDoneCond isDone;
#endif

	/*
	 * Check if all append subplans were pruned. In that case there is nothing
	 * to do.
	 */
	if (state->num_append_subplans == 0)
		return NULL;

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	ResetExprContext(econtext);

	while (true)
	{
		subslot = ExecProcNode(linitial(node->custom_ps));

		if (TupIsNull(subslot))
			return NULL;

		if (!node->ss.ps.ps_ProjInfo)
			return subslot;

		econtext->ecxt_scantuple = subslot;

#if PG10
		return ExecProject(node->ss.ps.ps_ProjInfo);
#elif PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#endif
	}
}

static void
ca_append_end(CustomScanState *node)
{
	if (node->custom_ps != NIL)
		ExecEndNode(linitial(node->custom_ps));
}

static void
ca_append_rescan(CustomScanState *node)
{
#if PG96
	node->ss.ps.ps_TupFromTlist = false;
#endif
	if (node->custom_ps != NIL)
		ExecReScan(linitial(node->custom_ps));
}

static void
ca_append_explain(CustomScanState *node,
				  List *ancestors,
				  ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	ConstraintAwareAppendInfo *info = (ConstraintAwareAppendInfo *) linitial(cscan->custom_private);

	ExplainPropertyText("Hypertable", get_rel_name(info->hypertable_relid), es);
	ExplainPropertyInteger("Chunks left after exclusion", state->num_append_subplans, es);
}


static CustomExecMethods constraint_aware_append_state_methods = {
	.BeginCustomScan = ca_append_begin,
	.ExecCustomScan = ca_append_exec,
	.EndCustomScan = ca_append_end,
	.ReScanCustomScan = ca_append_rescan,
	.ExplainCustomScan = ca_append_explain,
};

static Node *
constraint_aware_append_state_create(CustomScan *cscan)
{
	ConstraintAwareAppendState *state;
	Append	   *append = linitial(cscan->custom_plans);

	state = (ConstraintAwareAppendState *) newNode(sizeof(ConstraintAwareAppendState),
												   T_CustomScanState);
	state->csstate.methods = &constraint_aware_append_state_methods;
	state->subplan = &append->plan;

	return (Node *) state;
}

static CustomScanMethods constraint_aware_append_plan_methods = {
	.CustomName = CUSTOM_NAME,
	.CreateCustomScanState = constraint_aware_append_state_create,
};

static Plan *
constraint_aware_append_plan_create(PlannerInfo *root,
									RelOptInfo *rel,
									struct CustomPath *path,
									List *tlist,
									List *clauses,
									List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan	   *subplan = linitial(custom_plans);
	ConstraintAwareAppendPath *ca_path = (ConstraintAwareAppendPath *) path;
	ConstraintAwareAppendInfo *ca_info = ca_path->info;

	ca_info->clauses = list_copy(clauses);
	cscan->scan.scanrelid = 0;	/* Not a real relation we are scanning */
	cscan->scan.plan.targetlist = tlist;	/* Target list we expect as output */
	cscan->custom_plans = custom_plans;
	cscan->custom_private = list_make1(ca_info);
	cscan->custom_scan_tlist = subplan->targetlist; /* Target list of tuples
													 * we expect as input */
	cscan->flags = path->flags;
	cscan->methods = &constraint_aware_append_plan_methods;

	return &cscan->scan.plan;
}

static CustomPathMethods constraint_aware_append_path_methods = {
	.CustomName = CUSTOM_NAME,
	.PlanCustomPath = constraint_aware_append_plan_create,
};

typedef struct PathSortInfo
{
	PlannerInfo *root;
	Hypertable *ht;
	Dimension *dim;
	MergeAppendPath *ma;
	PathKey *key;
	ConstraintAwareAppendInfo *ca_info;
	Cost startup_cost;
	Cost total_cost;
	Cache *ccache;
} PathSortInfo;


typedef struct ChunkPath {
	Chunk *chunk;
	DimensionSlice *slice;
	Path *path;
	AppendRelInfo *appendinfo;
} ChunkPath;

static int
compar_path(void *ctx, const void *left, const void *right)
{
	PathSortInfo *sortinfo = ctx;
	const ChunkPath *pl = left;
	const ChunkPath *pr = right;
	bool asc = sortinfo->key->pk_opfamily == BTLessStrategyNumber;

	if (pl->slice->fd.range_start == pr->slice->fd.range_start &&
		pl->slice->fd.range_end == pr->slice->fd.range_end)
		return 0;

	if (pl->slice->fd.range_start < pr->slice->fd.range_start)
		return asc ? -1 : 1;

	return asc ? 1 : -1;
}


static RangeTblEntry *
make_subquery_rte(PlannerInfo *root, List *subpaths)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	ListCell *lc;
	Path *path = linitial(subpaths);
	Query *query;
	RangeTblRef *rte_ref;

	rte->inh = false;
	rte->rtekind = RTE_SUBQUERY;
	rte->relkind = RELKIND_RELATION;
	rte->eref = root->simple_rte_array[path->parent->relid]->eref;
	root->parse->rtable = lappend(root->parse->rtable, rte);

	query = rte->subquery = makeNode(Query);
	query->commandType = CMD_SELECT;
	query->targetList = root->parse->targetList;
	query->rtable = NIL;

	rte_ref = makeNode(RangeTblRef);
	rte_ref->rtindex = 1;
	query->jointree = makeFromExpr(list_make1(rte_ref), NULL);

	foreach(lc, subpaths)
	{
		Path *path = lfirst(lc);
		RangeTblEntry *crte = root->simple_rte_array[path->parent->relid];

		// Build subquery expression
		query->rtable = lappend(query->rtable, crte);

	}
	return rte;
}

static RelOptInfo *
add_other_rel(PlannerInfo *root, int relid, RangeTblEntry *rte, RelOptInfo *parent)
{
	root->simple_rte_array[relid] = rte;
	return build_simple_rel(root, relid, parent);
}

static int
expand_relation_arrays(PlannerInfo *root, int num_extra_relations)
{
	int idx = root->simple_rel_array_size;
	int i;

	root->simple_rel_array_size += num_extra_relations;
	root->simple_rel_array = repalloc(root->simple_rel_array,
									  root->simple_rel_array_size * sizeof(RelOptInfo *));
	root->simple_rte_array = repalloc(root->simple_rte_array,
									  root->simple_rel_array_size * sizeof(RangeTblEntry *));

	for (i = idx; i < root->simple_rel_array_size; i++)
	{
		root->simple_rel_array[i] = NULL;
		root->simple_rte_array[i] = NULL;
	}

	return idx;
}

static ChunkPath *
create_and_sort_chunk_paths(PathSortInfo *sortinfo, List *paths)
{
	int num_paths = list_length(paths);
	ChunkPath *chunkpaths = palloc(sizeof(ChunkPath) * num_paths);
	ListCell *lc_path, *lc_rel;
	int i = 0;

	forboth(lc_path, paths, lc_rel, sortinfo->ca_info->append_rel_list) {
		Path *path = lfirst(lc_path);
		AppendRelInfo *relinfo = lfirst(lc_rel);
		ChunkPath *cpath = &chunkpaths[i++];
		RangeTblEntry *rte;

		if (path->parent->reloptkind != RELOPT_OTHER_MEMBER_REL)
			elog(ERROR, "relation is not a hypertable chunk");

		rte = planner_rt_fetch(path->parent->relid, sortinfo->root);

		if (NULL == rte)
			elog(ERROR, "no range table entry for index %u", path->parent->relid);

		//cpath->chunk = chunk_get_by_relid(rte->relid, sortinfo->ht->space->num_dimensions, false);
		cpath->chunk = chunk_cache_get(sortinfo->ccache, rte->relid, sortinfo->ht->space->num_dimensions);

		/* FIXME: Could we expect something else than a chunk here? */
		if (NULL == cpath->chunk)
			elog(ERROR, "relation %u is not a hypertable chunk", rte->relid);

		cpath->slice = hypercube_get_slice_by_dimension_id(cpath->chunk->cube, sortinfo->dim->fd.id);
		cpath->path = path;
		cpath->appendinfo = relinfo;
		Assert(relinfo->child_relid == path->parent->relid);
		Assert(cpath->slice != NULL);
	}

	qsort_r(chunkpaths, num_paths, sizeof(ChunkPath), sortinfo, compar_path);

	return chunkpaths;
}

static List *
sort_paths(PathSortInfo *sortinfo, List *paths)
{
	int num_paths = list_length(paths);
	ChunkPath *chunkpaths;
	List *merge_append_paths = NIL;
	List *append_rel_list = NIL;
	int i = 0;

	chunkpaths = create_and_sort_chunk_paths(sortinfo, paths);

	paths = NIL;

	/*
	 * Chunk paths are now sorted on time. We need to find paths that are
	 * overlapping in the time dimension and then replace each overlapping set
	 * of paths with a MergeAppend. Paths can be overlapping if the, e.g., are
	 * part of the same "space" partition.
	 */
	for (i = 0; i < num_paths; i++)
	{
		ChunkPath *cpath = &chunkpaths[i];
		List *overlapping_paths = NIL;
		MergeAppendPath *mappend = NULL;
		Path *path = cpath->path;
		int j;

		for (j = i + 1; j < num_paths; j++)
		{
			ChunkPath *cpath_next = &chunkpaths[j];

			/* Does the next path overlap with the previou one? */
			if (!dimension_slices_collide(cpath->slice, cpath_next->slice))
				break;

			if (NULL == mappend)
			{
				/* Use the previous MergeAppendPath as a template, but do not
				 * copy the subpaths */
				mappend = makeNode(MergeAppendPath);
				memcpy(&mappend->path, &sortinfo->ma->path, sizeof(Path));
				mappend->path.startup_cost = cpath->path->startup_cost;
				mappend->path.total_cost = cpath->path->total_cost;
				mappend->path.rows = cpath->path->rows;
				mappend->subpaths = lappend(mappend->subpaths, cpath->path);
				append_rel_list = lappend(append_rel_list, cpath->appendinfo);
				path = &mappend->path;
				merge_append_paths = lappend(merge_append_paths, mappend);
			}

			if (list_length(overlapping_paths) == 0)
				overlapping_paths = lappend(overlapping_paths, cpath);

			overlapping_paths = lappend(overlapping_paths, cpath_next);

			append_rel_list = lappend(append_rel_list, cpath_next->appendinfo);
			mappend->subpaths = lappend(mappend->subpaths, cpath_next->path);
			mappend->path.startup_cost += cpath_next->path->startup_cost;
			mappend->path.total_cost += cpath_next->path->total_cost;
			mappend->path.rows += cpath_next->path->rows;
			cpath = &chunkpaths[++i];
		}

		sortinfo->startup_cost += path->startup_cost;
		sortinfo->total_cost += path->total_cost;
		paths = lappend(paths, path);
	}


	/*
	 * Build RelOptInfos for the MergeAppend nodes and do cost calculation
	 */
	if (list_length(merge_append_paths) > 0)
	{
		ListCell *lc;

		i = expand_relation_arrays(sortinfo->root, list_length(merge_append_paths));

		foreach(lc, merge_append_paths)
		{
			MergeAppendPath *map = lfirst(lc);
			RangeTblEntry *rte = make_subquery_rte(sortinfo->root, map->subpaths);
			RelOptInfo *rel = add_other_rel(sortinfo->root, i, rte, NULL);
			ListCell *lc;

			map->path.parent = rel;
			rel->pathlist = lappend(rel->pathlist, map);

			foreach(lc, map->subpaths)
			{
				Path *child = lfirst(lc);
				bms_add_members(rel->relids, child->parent->relids);
				rel->rows += child->rows;
			}

			sortinfo->root->parse->rtable = lappend(sortinfo->root->parse->rtable, rte);
			cost_merge_append(&map->path,
							  sortinfo->root,
							  map->path.pathkeys,
							  list_length(map->subpaths),
							  map->path.startup_cost,
							  map->path.total_cost,
							  map->path.rows);
			i++;
		}
	}

	sortinfo->ca_info->append_rel_list = append_rel_list;

	return paths;
}

static Path *
make_sorted_append(PlannerInfo *root, Hypertable *ht, Path *path, ConstraintAwareAppendInfo *ca_info)
{
	MergeAppendPath *ma = (MergeAppendPath *) path;
	PathKey *key;
	EquivalenceMember *ecm;

	if (list_length(path->pathkeys) != 1)
		return path;

	key = linitial(path->pathkeys);
	ecm = linitial(key->pk_eclass->ec_members);

	if (IsA(ecm->em_expr, Var)) {
		Var *v = (Var *) ecm->em_expr;
		RangeTblEntry *rte = planner_rt_fetch(v->varno, root);
		const char *attname = get_relid_attribute_name(rte->relid, v->varattno);
		Dimension *dim = hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_OPEN, attname);

		if (NULL != dim)
		{
			AppendPath *append;
			PathSortInfo info = {
				.root = root,
				.ht = ht,
				.dim = dim,
				.key = key,
				.ma = ma,
				.ca_info = ca_info,
				.ccache = chunk_cache_pin(),
			};

			append = makeNode(AppendPath);
			/* Copy basic info from the original merge append */
			memcpy(&append->path, path, sizeof(Path));
			append->path.type = T_AppendPath;
			append->path.pathtype = T_Append;
			append->subpaths = sort_paths(&info, ma->subpaths);
			append->path.rows = ma->path.rows;
			append->path.startup_cost = info.startup_cost;
			append->path.total_cost = info.total_cost;
#if PG10
			append->partitioned_rels = ma->partitioned_rels;
#endif
			cache_release(info.ccache);

			return &append->path;
		}
	}

	return path;
}

/*
 * Preprocess all append relations.
 *
 * Since we are doing constraint exclusion at execution time, we need to save
 * some information about append relations that we need when executing the
 * query. We need the list of append relations to match the subpath list so that
 * we can efficiently iterate them in tandem. However, the list of append
 * relations in the PlannerInfo doesn't match the Path nodes since the planner
 * might already have pruned the subpaths list using regular constraint
 * exclusion. Further, we would like to remove the hypertable's root table from
 * the plan, since it doesn't have any tuples. Therefore, we create a new append
 * relations list that matches the subpaths list.
 */
static inline List *
preprocess_append_relations(PlannerInfo *root, List *subpaths, ConstraintAwareAppendInfo *info)
{
	ListCell *lc, *lc_prev = NULL, *lc_info = list_head(root->append_rel_list);
	List *newpaths = NIL;

	foreach(lc, subpaths)
	{
		Path *path = lfirst(lc);
		Oid	 reloid = root->simple_rte_array[path->parent->relid]->relid;
		AppendRelInfo *apprelinfo;

		/* Remove the main/root table since it has no tuples */
		if (reloid == info->hypertable_relid)
			continue;

		for (; NULL != lc_info; lc_info = lnext(lc_info))
		{
			apprelinfo = lfirst(lc_info);

			if (apprelinfo->child_relid == path->parent->relid)
				break;
		}

		if (NULL == lc_info)
			elog(ERROR, "no append relation info for relation %s", get_rel_name(reloid));

		info->append_rel_list = lappend(info->append_rel_list, apprelinfo);
		newpaths = lappend(newpaths, path);
		lc_prev = lc;
	}

	return newpaths;
}

static Path *
transform_append_path(PlannerInfo *root, Hypertable *ht, Path *path, ConstraintAwareAppendInfo *info)
{
	/*
	 * Remove the main table from the Append's subpaths since it cannot contain
	 * any tuples
	 */
	switch (nodeTag(path))
	{
		case T_AppendPath:
			{
				AppendPath *append = (AppendPath *) path;
				append->subpaths = preprocess_append_relations(root, append->subpaths, info);
				break;
			}
		case T_MergeAppendPath:
			{
				MergeAppendPath *append = (MergeAppendPath *) path;
				append->subpaths = preprocess_append_relations(root, append->subpaths, info);
				path = make_sorted_append(root, ht, path, info);
				break;
			}
		default:
			elog(ERROR, "unexpected node type %u", nodeTag(path));
			break;
	}

	return path;
}

Path *
constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath, bool do_exclusion)
{
	ConstraintAwareAppendPath *path;
	ConstraintAwareAppendInfo *info = constraint_aware_append_info_create();

	info->hypertable_relid = ht->main_table_relid;
	info->do_exclusion = do_exclusion;

	subpath = transform_append_path(root, ht, subpath, info);

	path = (ConstraintAwareAppendPath *) newNode(sizeof(ConstraintAwareAppendPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;
	path->info = info;

	/*
	 * Set flags. We can set CUSTOMPATH_SUPPORT_BACKWARD_SCAN and
	 * CUSTOMPATH_SUPPORT_MARK_RESTORE. The only interesting flag is the first
	 * one (backward scan), but since we are not scanning a real relation we
	 * need not indicate that we support backward scans. Lower-level index
	 * scanning nodes will scan backward if necessary, so once tuples get to
	 * this node they will be in a given order already.
	 */
	path->cpath.flags = 0;
	path->cpath.custom_paths = list_make1(subpath);
	path->cpath.methods = &constraint_aware_append_path_methods;

	return &path->cpath.path;
}

void
_constraint_aware_append_init(void)
{
	RegisterExtensibleNodeMethods(&constraint_aware_append_info_methods);
}

void
_constraint_aware_append_fini(void)
{
}
