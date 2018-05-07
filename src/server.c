#include <postgres.h>
#include <fmgr.h>
#include <foreign/foreign.h>
#include <nodes/parsenodes.h>
#include <nodes/makefuncs.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <miscadmin.h>

#include "server.h"
#include "compat.h"

//Datum add_server(PG_FUNCTION_ARGS);

TS_FUNCTION_INFO_V1(server_add);

Datum
server_add(PG_FUNCTION_ARGS)
{
	Name server_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_NAME(0);
	Name hostname = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1);
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int16 port = PG_ARGISNULL(3) ? 5432 : PG_GETARG_INT16(3);
	ForeignServer *server;

	if (NULL == server_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid server name"))));

	server = GetForeignServerByName(NameStr(*server_name), true);

	if (NULL == server)
	{
		ObjectAddress objaddr;
		CreateForeignServerStmt stmt = {
			.type = T_CreateForeignServerStmt,
			.servername = NameStr(*server_name),
			.options = list_make3(makeDefElem("host", (Node *) makeString(NameStr(*server_name)), -1),
								  makeDefElem("dbname", (Node *) makeString(pstrdup(dbname)), -1),
								  makeDefElem("port", (Node *) makeInteger(port), -1)),
		};

		if (NULL == hostname)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("invalid host"),
					  (errhint("A host must be specified when a foreign server does not already exist.")))));


		objaddr = CreateForeignServer(&stmt);
		server = GetForeignServer(objaddr.objectId);
	}

	Assert(server != NULL);

	PG_RETURN_VOID();
}
