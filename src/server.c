#include <postgres.h>
#include <access/xact.h>
//#include <foreign/foreign.h>
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
#include "scanner.h"

static void
server_insert_relation(Relation rel,
					   const char *server_name,
					   const char *dbname,
					   const char *host,
					   int32 port)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum values[Natts_server];
	bool nulls[Natts_server] = { false };
	CatalogSecurityContext sec_ctx;

	values[Anum_server_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(server_name));
	values[Anum_server_dbname - 1] = DirectFunctionCall1(namein, CStringGetDatum(dbname));
	values[Anum_server_port - 1] = Int32GetDatum(port);
	values[Anum_server_host - 1] = DirectFunctionCall1(textin, CStringGetDatum(host));

	catalog_become_owner(catalog_get(), &sec_ctx);
	values[Anum_dimension_id - 1] = Int32GetDatum(catalog_table_next_seq_id(catalog_get(), SERVER));

	PG_TRY();
	{
		catalog_insert_values(rel, desc, values, nulls);
	}
	PG_CATCH();
	{
		/* TODO: find a way to check for unique violation error */
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("server already exists")));
	}
	PG_END_TRY();

	catalog_restore_user(&sec_ctx);
}

static int
server_scan_internal(ScanKeyData *scankey,
					 int nkeys,
					 int indexid,
					 tuple_found_func tuple_found,
					 void *data,
					 int limit,
					 LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[SERVER].id,
		.index = catalog->tables[SERVER].index_ids[indexid],
		.nkeys = nkeys,
		.limit = limit,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanctx);
}

static bool
server_tuple_found(TupleInfo *ti, void *data)
{
	Server **server = (Server **) data;
	Datum values[Natts_server];
	bool nulls[Natts_server] = { false };
	Size hostlen;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	hostlen =  DirectFunctionCall1(textlen, values[Anum_server_host - 1]);
	*server = palloc(sizeof(Server) + hostlen);
	(*server)->fd.id = DatumGetInt32(values[Anum_server_id - 1]);
	namecpy(&(*server)->fd.name, DatumGetName(values[Anum_server_name - 1]));
	namecpy(&(*server)->fd.dbname, DatumGetName(values[Anum_server_dbname - 1]));
	(*server)->fd.port = DatumGetInt32(values[Anum_server_port - 1]);
	memcpy(&(*server)->fd.host, DatumGetTextP(values[Anum_server_host - 1]), VARHDRSZ + hostlen);

	return false;
}

Server *
server_get_by_name(const char *server_name)
{
	Server *server = NULL;
	ScanKeyData scankey[1];

	/* Perform an index scan on hypertable_id. */
	ScanKeyInit(&scankey[0],
				Anum_server_name_idx_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(server_name)));

	server_scan_internal(scankey,
						 1,
						 SERVER_NAME_IDX,
						 server_tuple_found,
						 &server,
						 1,
						 AccessShareLock);

	return server;
}


static void
server_insert(const char *server_name,
			  const char *dbname,
			  const char *host,
			  int32 port)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;

	rel = heap_open(catalog->tables[SERVER].id, RowExclusiveLock);
	server_insert_relation(rel, server_name, dbname, host, port);
	heap_close(rel, RowExclusiveLock);
}

TS_FUNCTION_INFO_V1(server_add);

Datum
server_add(PG_FUNCTION_ARGS)
{
	Name server_name = PG_ARGISNULL(0) ? NULL : PG_GETARG_NAME(0);
	const char *host = PG_ARGISNULL(1) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(1));
	const char *dbname = PG_ARGISNULL(2) ? get_database_name(MyDatabaseId) : PG_GETARG_CSTRING(2);
	int32 port = PG_ARGISNULL(3) ? 5432 : PG_GETARG_INT32(3);
	/* ForeignServer *server; */
	Server *server;

	if (NULL == server_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid server name"))));

	if (port < 1 || port > PG_UINT16_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("invalid port"),
				  errhint("The port must be between 1 and %u", PG_UINT16_MAX))));

	elog(NOTICE, "host is %s", host);
	server_insert(NameStr(*server_name),
				  dbname,
				  host,
				  port);

	server = server_get_by_name(NameStr(*server_name));

	if (NULL == server)
		elog(NOTICE, "Added server not found");


	elog(NOTICE, "server: %s %s %s %d",
		 NameStr(server->fd.name), NameStr(server->fd.dbname), text_to_cstring(&server->fd.host), server->fd.port);
	/*
	server = GetForeignServerByName(

	if (NULL == server)
	{
		ObjectAddress objaddr;
		CreateForeignServerStmt stmt = {
			.type = T_CreateForeignServerStmt,
			.servername = NameStr(*server_name),
			.fdwname = "timescaledb_fdw",
			.options = list_make3(makeDefElem("host", (Node *) makeString(host), -1),
								  makeDefElem("dbname", (Node *) makeString(dbname), -1),
								  makeDefElem("port", (Node *) makeInteger(port), -1)),
		};

		if (NULL == hostname)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("invalid host"),
					  (errhint("A hostname or IP address must be specified when a foreign server does not already exist.")))));


		objaddr = CreateForeignServer(&stmt);
		CommandCounterIncrement();
		server = GetForeignServer(objaddr.objectId);
	}

	Assert(server != NULL);
	*/

	PG_RETURN_VOID();
}
