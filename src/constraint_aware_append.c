#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>
#include <optimizer/plancat.h>
#include <optimizer/clauses.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <optimizer/pathnode.h>
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
	List	   *append_rel_info = lsecond(cscan->custom_private);
	List	   *restrictinfos = constify_restrictinfos(lthird(cscan->custom_private));
	bool       do_exclusion = linitial_int(lfourth(cscan->custom_private));
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
				elog(NOTICE, "Num plans %u", list_length(old_appendplans));
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
			elog(ERROR, "Invalid plan %d", nodeTag(plan));
	}

	lc_info = list_head(append_rel_info);

	foreach(lc_plan, old_appendplans)
	{
		Plan       *subplan = lfirst(lc_plan);
		AppendRelInfo *appinfo = lfirst(lc_info);

		if (is_scan(subplan))
		{
			Scan *scan = (Scan *) subplan;

			if (!do_exclusion ||
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
			ListCell *lc;
			List *mergeplans = ma->mergeplans;

			ma->mergeplans = NIL;

			foreach (lc, mergeplans)
			{
				Plan *ma_plan = lfirst(lc);
				AppendRelInfo *appinfo = lfirst(lc_info);

				if (is_scan(ma_plan))
				{
					Scan *scan = (Scan *) ma_plan;

					if (!do_exclusion ||
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

	elog(NOTICE, "Num scans=%d", num_scans);

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
	//CustomScan *cscan = (CustomScan *) state->csstate.ss.ps.plan;

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
	{
		ExecEndNode(linitial(node->custom_ps));
	}
}

static void
ca_append_rescan(CustomScanState *node)
{
#if PG96
	node->ss.ps.ps_TupFromTlist = false;
#endif
	if (node->custom_ps != NIL)
	{
		ExecReScan(linitial(node->custom_ps));
	}
}

static void
ca_append_explain(CustomScanState *node,
				  List *ancestors,
				  ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	Oid			relid = linitial_oid(linitial(cscan->custom_private));

	ExplainPropertyText("Hypertable", get_rel_name(relid), es);
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
	.CustomName = "ConstraintAwareAppend",
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
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	ConstraintAwareAppendPath *ca_path = (ConstraintAwareAppendPath *) path;

	cscan->scan.scanrelid = 0;	/* Not a real relation we are scanning */
	cscan->scan.plan.targetlist = tlist;	/* Target list we expect as output */
	cscan->custom_plans = custom_plans;
	cscan->custom_private = list_make4(list_make1_oid(rte->relid),
									   list_copy(root->append_rel_list),
									   list_copy(clauses),
									   list_make1_int(ca_path->perform_exclusion));
	cscan->custom_scan_tlist = subplan->targetlist; /* Target list of tuples
													 * we expect as input */
	cscan->flags = path->flags;
	cscan->methods = &constraint_aware_append_plan_methods;

	return &cscan->scan.plan;
}

static CustomPathMethods constraint_aware_append_path_methods = {
	.CustomName = "ConstraintAwareAppend",
	.PlanCustomPath = constraint_aware_append_plan_create,
};

static inline List *
remove_parent_subpath(PlannerInfo *root, List *subpaths, Oid parent_relid)
{
	Path	   *childpath;
	Oid			relid;

	childpath = linitial(subpaths);
	relid = root->simple_rte_array[childpath->parent->relid]->relid;

	if (relid == parent_relid)
		subpaths = list_delete_first(subpaths);

	return subpaths;
}

typedef struct PathSortInfo
{
	PlannerInfo *root;
	Hypertable *ht;
	Dimension *dim;
	MergeAppendPath *ma;
	PathKey *key;
	Cost total_cost;
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

static List *
sort_paths(PathSortInfo *sortinfo, List *paths)
{
	int num_paths = list_length(paths);
	ChunkPath *chunkpaths = palloc(sizeof(ChunkPath) * num_paths);
	ListCell *lc_path, *lc_appinfo;
	List *merge_append_paths = NIL;
	List *append_rel_list = NIL;
	int i = 0;

	lc_appinfo = list_head(sortinfo->root->append_rel_list);

	foreach(lc_path, paths) {
		Path *path = lfirst(lc_path);
		AppendRelInfo *appendinfo;
		ChunkPath *cpath = &chunkpaths[i++];
		RangeTblEntry *rte;

		/* We need the match up AppendRelInfos with the corresponding path node
		 * in order to later do runtime constraint exclusion. We can expect
		 * relations in the append_rel_list to be in the same order as the
		 * subpaths of the append node, but due to constraint exclusion there
		 * might be more AppendRelInfos than subpaths. We need to skip those
		 * AppendRelInfos that were exluded */
		for (; lc_appinfo != NULL; lc_appinfo = lnext(lc_appinfo))
		{
			appendinfo = lfirst(lc_appinfo);

			if (appendinfo->child_relid == path->parent->relid)
				break;
		}

		if (lc_appinfo == NULL)
			elog(ERROR, "no AppendRelInfo found for relation with RTE index %u", path->parent->relid);

		/* FIXME: Could we expect something else than a chunk here? */
		if (path->parent->reloptkind != RELOPT_OTHER_MEMBER_REL)
		{
			elog(NOTICE, "not a chunk");
			return paths;
		}

		/* FIXME: do proper cost calc. */
		sortinfo->total_cost += (path->total_cost / 3);
		rte = sortinfo->root->simple_rte_array[path->parent->relid];

		if (NULL == rte)
			elog(ERROR, "no range table entry for index %u", path->parent->relid);

		cpath->chunk = chunk_get_by_relid(rte->relid, sortinfo->ht->space->num_dimensions, true);

		/* FIXME: Could we expect something else than a chunk here? */
		if (NULL == cpath->chunk)
			elog(ERROR, "relation %u is not a hypertable chunk", rte->relid);

		cpath->slice = hypercube_get_slice_by_dimension_id(cpath->chunk->cube, sortinfo->dim->fd.id);
		cpath->path = path;
		cpath->appendinfo = appendinfo;
		Assert(appendinfo->child_relid == path->parent->relid);
		Assert(cpath->slice != NULL);
	}

	qsort_r(chunkpaths, num_paths, sizeof(ChunkPath), sortinfo, compar_path);

	paths = NIL;

	for (i = 0; i < num_paths; i++)
	{
		ChunkPath *cpath = &chunkpaths[i];
		MergeAppendPath *mappend = NULL;
		Path *path = cpath->path;
		int j;

		append_rel_list = lappend(append_rel_list, cpath->appendinfo);
		elog(NOTICE, "Handling path for chunk %s", NameStr(cpath->chunk->fd.table_name));

		for (j = i + 1; j < num_paths; j++)
		{
			ChunkPath *cpath_next = &chunkpaths[j];

			if (!dimension_slices_collide(cpath->slice, cpath_next->slice))
				break;

			append_rel_list = lappend(append_rel_list, cpath_next->appendinfo);

			if (NULL == mappend)
			{
				/* Use the previous MergeAppendPath as a template, but do not
				 * copy the subpaths */
				mappend = makeNode(MergeAppendPath);
				memcpy(&mappend->path, &sortinfo->ma->path, sizeof(Path));
				mappend->path.startup_cost = 0;
				mappend->path.total_cost += cpath->path->total_cost;
				mappend->subpaths = lappend(mappend->subpaths, cpath->path);
				path = &mappend->path;
				merge_append_paths = lappend(merge_append_paths, mappend);
			}

			mappend->subpaths = lappend(mappend->subpaths, cpath_next->path);
			mappend->path.total_cost += cpath_next->path->total_cost;
			cpath = &chunkpaths[++i];
		}

		paths = lappend(paths, path);
	}

	if (list_length(merge_append_paths) > 0)
	{
		i = expand_relation_arrays(sortinfo->root, list_length(merge_append_paths));

		foreach(lc_path, merge_append_paths)
		{
			MergeAppendPath *map = lfirst(lc_path);
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
			i++;
		}
	}

	sortinfo->root->append_rel_list = append_rel_list;

	return paths;
}

static Path *
maybe_transform_merge_append_into_sorted_append(PlannerInfo *root, Hypertable *ht, Path *path)
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
		RangeTblEntry *rte = root->simple_rte_array[v->varno];
		const char *attname = get_relid_attribute_name(rte->relid, v->varattno);
		Dimension *dim = hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_OPEN, attname);

		if (NULL != dim)
		{
			AppendPath *append = makeNode(AppendPath);
			PathSortInfo info = {
				.root = root,
				.ht = ht,
				.dim = dim,
				.key = key,
				.ma = ma,
			};

			memcpy(&append->path, path, sizeof(Path));
			append->path.type = T_AppendPath;
			append->path.pathtype = T_Append;
			append->path.rows = ma->path.rows;
			append->path.pathtarget = ma->path.pathtarget;
			append->path.startup_cost = 0;
#if PG10
			append->partitioned_rels = ma->partitioned_rels;
#endif
			append->subpaths = sort_paths(&info, ma->subpaths);
			append->path.total_cost = info.total_cost;

			return &append->path;
		}
	}

	return path;
}
/*
static void
print_subpaths(List *subpaths)
{
	ListCell *lc;
	int i = 0;

	foreach(lc, subpaths) {
		Path *path = lfirst(lc);

		elog(NOTICE, "Subpath %d: relididx=%u",
			 i++, path->parent->relid);
	}
}

static void
print_appendrelinfo(PlannerInfo *root)
{
	ListCell *lc;
	int i = 0;

	foreach(lc, root->append_rel_list)
	{
		AppendRelInfo *info = lfirst(lc);
		RelOptInfo *relopt = root->simple_rel_array[info->child_relid];
		char *name = "";

		if (IS_SIMPLE_REL(relopt))
			name = get_rel_name(root->simple_rte_array[relopt->relid]->relid);

		elog(NOTICE, "AppendRelinfo %d: relididx=%u name=%s", i++, info->child_relid, name);
	}
}
*/
Path *
constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath, bool do_exclusion)
{
	ConstraintAwareAppendPath *path;
	AppendRelInfo *appinfo;
	Oid			relid;

	/*
	 * Remove the main table from the append_rel_list and Append's subpaths
	 * since it cannot contain any tuples
	 */
	switch (nodeTag(subpath))
	{
		case T_AppendPath:
			{
				AppendPath *append = (AppendPath *) subpath;
				append->subpaths = remove_parent_subpath(root, append->subpaths, ht->main_table_relid);
				break;
			}
		case T_MergeAppendPath:
			{
				MergeAppendPath *append = (MergeAppendPath *) subpath;
				append->subpaths = remove_parent_subpath(root, append->subpaths, ht->main_table_relid);
				subpath = maybe_transform_merge_append_into_sorted_append(root, ht, subpath);
				break;
			}
		default:
			elog(ERROR, "Invalid node type %u", nodeTag(subpath));
			break;
	}

	path = (ConstraintAwareAppendPath *) newNode(sizeof(ConstraintAwareAppendPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;
	path->perform_exclusion = do_exclusion;

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

	appinfo = linitial(root->append_rel_list);
	relid = root->simple_rte_array[appinfo->child_relid]->relid;

	if (relid == ht->main_table_relid)
		root->append_rel_list = list_delete_first(root->append_rel_list);

	return &path->cpath.path;
}
