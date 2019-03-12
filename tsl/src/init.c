/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include <export.h>
#include <cross_module_fn.h>
#include <license_guc.h>

#include "planner.h"
#include "gapfill/gapfill.h"

#include "license.h"
#include "reorder.h"
#include "telemetry.h"
#include "bgw_policy/job.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/drop_chunks_api.h"
#include "server.h"
#include "fdw/timescaledb_fdw.h"
#include "chunk_api.h"
#include "hypertable.h"
#include "compat.h"

#if !PG96
#include "remote/connection_cache.h"
#include "remote/dist_txn.h"
#include "remote/txn_id.h"
#include "remote/txn_resolve.h"
#include "server_dispatch.h"
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef APACHE_ONLY
#error "cannot compile the TSL for ApacheOnly mode"
#endif

extern void PGDLLEXPORT _PG_init(void);

static void module_shutdown(void);
static bool enterprise_enabled_internal(void);
static bool check_tsl_loaded(void);

static void
cache_syscache_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	/*
	 * Using hash_value it is possible to do more fine grained validation in
	 * the future see `postgres_fdw` connection management for an example. For
	 * now, invalidate the entire cache.
	 */
#if !PG96
	remote_connection_cache_invalidate_callback();
#endif
}

#if PG96

static Datum
empty_fn(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

static void
error_not_supported(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function is not supported under the current PostgreSQL version %s",
					PG_VERSION_STR),
			 errhint("Upgrade PostgreSQL to version 10 or greater.")));
	pg_unreachable();
}

static Datum
error_not_supported_default_fn(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function \"%s\" is not supported under the current PostgreSQL version %s",
					get_func_name(fcinfo->flinfo->fn_oid),
					PG_VERSION_STR),
			 errhint("Upgrade PostgreSQL to version 10 or greater.")));
	pg_unreachable();
}

static List *
error_get_serverlist_not_supported(void)
{
	error_not_supported();
	pg_unreachable();
}

static void
error_hypertable_make_distributed_not_supported(Hypertable *ht, ArrayType *servers)
{
	error_not_supported();
	pg_unreachable();
}

static void
error_create_chunk_on_servers_not_supported(Chunk *chunk, Hypertable *ht)
{
	error_not_supported();
	pg_unreachable();
}

static Path *
error_server_dispatch_path_create_not_supported(PlannerInfo *root, ModifyTablePath *mtpath,
												Index hypertable_rti, int subpath_index)
{
	error_not_supported();
	pg_unreachable();
}

#endif /* PG96 */

/*
 * Cross module function initialization.
 *
 * During module start we set ts_cm_functions to point at the tsl version of the
 * function registry.
 *
 * NOTE: To ensure that your cross-module function has a correct default, you
 * must also add it to ts_cm_functions_default in cross_module_fn.c in the
 * Apache codebase.
 */
CrossModuleFunctions tsl_cm_functions = {
	.tsl_license_on_assign = tsl_license_on_assign,
	.enterprise_enabled_internal = enterprise_enabled_internal,
	.check_tsl_loaded = check_tsl_loaded,
	.license_end_time = license_end_time,
	.print_tsl_license_expiration_info_hook = license_print_expiration_info,
	.module_shutdown_hook = module_shutdown,
	.add_tsl_license_info_telemetry = tsl_telemetry_add_license_info,
	.bgw_policy_job_execute = tsl_bgw_policy_job_execute,
	.add_drop_chunks_policy = drop_chunks_add_policy,
	.add_reorder_policy = reorder_add_policy,
	.remove_drop_chunks_policy = drop_chunks_remove_policy,
	.remove_reorder_policy = reorder_remove_policy,
	.create_upper_paths_hook = tsl_create_upper_paths_hook,
	.gapfill_marker = gapfill_marker,
	.gapfill_int16_time_bucket = gapfill_int16_time_bucket,
	.gapfill_int32_time_bucket = gapfill_int32_time_bucket,
	.gapfill_int64_time_bucket = gapfill_int64_time_bucket,
	.gapfill_date_time_bucket = gapfill_date_time_bucket,
	.gapfill_timestamp_time_bucket = gapfill_timestamp_time_bucket,
	.gapfill_timestamptz_time_bucket = gapfill_timestamptz_time_bucket,
	.alter_job_schedule = bgw_policy_alter_job_schedule,
	.reorder_chunk = tsl_reorder_chunk,
#if PG96
	.add_server = error_not_supported_default_fn,
	.delete_server = error_not_supported_default_fn,
	.attach_server = error_not_supported_default_fn,
	.show_chunk = error_not_supported_default_fn,
	.create_chunk = error_not_supported_default_fn,
	.create_chunk_on_servers = error_create_chunk_on_servers_not_supported,
	.get_servername_list = error_get_serverlist_not_supported,
	.hypertable_make_distributed = error_hypertable_make_distributed_not_supported,
	.timescaledb_fdw_handler = error_not_supported_default_fn,
	.timescaledb_fdw_validator = empty_fn,
	.set_rel_pathlist = NULL,
	.hypertable_should_be_expanded = NULL,
	.server_dispatch_path_create = error_server_dispatch_path_create_not_supported,
#else
	.add_server = server_add,
	.delete_server = server_delete,
	.attach_server = server_attach,
	.show_chunk = chunk_show,
	.create_chunk = chunk_create,
	.create_chunk_on_servers = chunk_api_create_on_servers,
	.get_servername_list = server_get_servername_list,
	.hypertable_make_distributed = hypertable_make_distributed,
	.timescaledb_fdw_handler = timescaledb_fdw_handler,
	.timescaledb_fdw_validator = timescaledb_fdw_validator,
	.remote_txn_id_in = remote_txn_id_in_pg,
	.remote_txn_id_out = remote_txn_id_out_pg,
	.remote_txn_heal_server = remote_txn_heal_server,
	.set_rel_pathlist = tsl_set_rel_pathlist,
	.hypertable_should_be_expanded = tsl_hypertable_should_be_expanded,
	.server_dispatch_path_create = server_dispatch_path_create,
#endif
	.cache_syscache_invalidate = cache_syscache_invalidate,
};

TS_FUNCTION_INFO_V1(ts_module_init);
/*
 * Module init function, sets ts_cm_functions to point at tsl_cm_functions
 */
PGDLLEXPORT Datum
ts_module_init(PG_FUNCTION_ARGS)
{
	ts_cm_functions = &tsl_cm_functions;

#if !PG96
	_remote_connection_cache_init();
	_remote_dist_txn_init();
#endif

	PG_RETURN_BOOL(true);
}

/*
 * Currently we disallow shutting down this submodule in a live session,
 * but if we did, this would be the function we'd use.
 */
static void
module_shutdown(void)
{
	/*
	 * Order of items should be strict reverse order of ts_module_init. Please
	 * document any exceptions.
	 */
#if !PG96
	_remote_dist_txn_fini();
	_remote_connection_cache_fini();
#endif

	ts_cm_functions = &ts_cm_functions_default;
}

/* Informative functions */

static bool
enterprise_enabled_internal(void)
{
	return license_enterprise_enabled();
}

static bool
check_tsl_loaded(void)
{
	return true;
}

PGDLLEXPORT void
_PG_init(void)
{
	/*
	 * In a normal backend, we disable loading the tsl until after the main
	 * timescale library is loaded, after which we enable it from the loader.
	 * In parallel workers the restore shared libraries function will load the
	 * libraries itself, and we bypass the loader, so we need to ensure that
	 * timescale is aware it can  use the tsl if needed. It is always safe to
	 * do this here, because if we reach this point, we must have already
	 * loaded the tsl, so we no longer need to worry about its load order
	 * relative to the other libraries.
	 */
	ts_license_enable_module_loading();
}
