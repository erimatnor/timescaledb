#include <postgres.h>
#include <access/xact.h>
#include <foreign/foreign.h>
#include <nodes/parsenodes.h>
#include <nodes/makefuncs.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <utils/builtins.h>
#include <miscadmin.h>
#include <fmgr.h>

#include "server.h"
#include "compat.h"
#include "catalog.h"

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

	PG_RETURN_VOID();
}
