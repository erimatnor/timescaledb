#include <postgres.h>
#include <access/xact.h>
#include <access/genam.h>
#include <access/htup_details.h>
#include <foreign/foreign.h>
#include <nodes/parsenodes.h>
#include <nodes/makefuncs.h>
#include <catalog/pg_foreign_server.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <miscadmin.h>
#include <fmgr.h>

#include "server.h"
#include "compat.h"
#include "catalog.h"
#include "fdw.h"

TS_FUNCTION_INFO_V1(server_add);

Datum
server_add(PG_FUNCTION_ARGS)
{
	Name server_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_NAME(0);
	const char *host = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(1));
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int32 port = PG_ARGISNULL(3) ? 5432 : PG_GETARG_INT32(3);
	ForeignServer *server;

	if (NULL == server_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid server name"))));

	if (port < 1 || port > PG_UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid port"),
				  errhint("The port must be between 1 and %u", PG_UINT16_MAX))));

	server = GetForeignServerByName(NameStr(*server_name), true);

	if (NULL == server)
	{
		ObjectAddress objaddr;
		CreateForeignServerStmt stmt = {
			.type = T_CreateForeignServerStmt,
			.servername = NameStr(*server_name),
			.fdwname = "timescaledb",
			.options = list_make3(makeDefElem("host", (Node *) makeString(pstrdup(host)), -1),
								  makeDefElem("dbname", (Node *) makeString(pstrdup(dbname)), -1),
								  makeDefElem("port", (Node *) makeInteger(port), -1)),
		};

		if (NULL == host)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("invalid host"),
					  (errhint("A hostname or IP address must be specified when a foreign server does not already exist.")))));


		objaddr = CreateForeignServer(&stmt);
		CommandCounterIncrement();
		server = GetForeignServer(objaddr.objectId);
	}

	Assert(server != NULL);

	// Need user mapping:
	//server_exec_on_all(list_make1(server->servername), "CREATE EXTENSION IF NOT EXISTS timescaledb");

	PG_RETURN_VOID();
}

List *
server_get_list(void)
{
	HeapTuple	tuple;
	ScanKeyData scankey[1];
	SysScanDesc scandesc;
	Relation rel;
	ForeignDataWrapper *fdw = GetForeignDataWrapperByName("timescaledb", false);
	List *servers = NIL;

	rel = heap_open(ForeignServerRelationId, AccessShareLock);

	ScanKeyInit(&scankey[0],
				Anum_pg_foreign_server_srvfdw,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fdw->fdwid));

	scandesc = systable_beginscan(rel, InvalidOid, false, NULL, 1, scankey);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_foreign_server form = (Form_pg_foreign_server) GETSTRUCT(tuple);
		servers = lappend(servers, NameStr(form->srvname));
	}

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return servers;
}

void
server_exec_on_all(List *servers, const char *stmt)
{
	ListCell *lc;

	/* TODO: 2PC */
	foreach (lc, servers)
	{
		ForeignServer *server = GetForeignServerByName(lfirst(lc), false);
		UserMapping *user = GetUserMapping(GetUserId(), server->serverid);
		PGconn *conn = GetConnection(user, false);
		PGresult *res;

		res = PQexec(conn, stmt);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			pgfdw_report_error(ERROR, res, conn, true, stmt);

		PQclear(res);
	}
}
