#include <postgres.h>
#include <utils/fmgroids.h>
#include <foreign/foreign.h>

#include "hypertable_server.h"
#include "scanner.h"

static void
hypertable_server_insert_relation(Relation rel,
								  int32 hypertable_id,
								  int32 server_hypertable_id,
								  Name server_name)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_hypertable_server];
	bool		nulls[Natts_hypertable_server] = {false};
	CatalogSecurityContext sec_ctx;

	values[Anum_hypertable_server_hypertable_id - 1] = Int32GetDatum(hypertable_id);
	values[Anum_hypertable_server_server_hypertable_id - 1] = Int32GetDatum(server_hypertable_id);
	values[Anum_hypertable_server_server_name - 1] = NameGetDatum(server_name);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);
}

void
hypertable_server_insert_multi(List *hypertable_servers)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	ListCell *lc;

	rel = heap_open(catalog->tables[HYPERTABLE_SERVER].id, RowExclusiveLock);

	foreach (lc, hypertable_servers)
	{
		HypertableServer *server = lfirst(lc);

		hypertable_server_insert_relation(rel,
										  server->fd.hypertable_id,
										  server->fd.server_hypertable_id,
										  &server->fd.server_name);
	}

	heap_close(rel, RowExclusiveLock);
}

static int
hypertable_server_scan_limit_internal(ScanKeyData *scankey,
									  int num_scankeys,
									  int indexid,
									  tuple_found_func on_tuple_found,
									  void *scandata,
									  int limit,
									  LOCKMODE lock)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[HYPERTABLE_SERVER].id,
		.index = CATALOG_INDEX(catalog, HYPERTABLE_SERVER, indexid),
		.nkeys = num_scankeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lock,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanctx);
}

static bool
hypertable_server_tuple_found(TupleInfo *ti, void *data)
{
	List **servers = data;
	Form_hypertable_server form = (Form_hypertable_server) GETSTRUCT(ti->tuple);
	HypertableServer *hypertable_server = palloc(sizeof(HypertableServer));
	ForeignServer *foreign_server = GetForeignServerByName(NameStr(form->server_name), false);

	memcpy(&hypertable_server->fd, form, sizeof(FormData_hypertable_server));
	hypertable_server->foreign_server_oid = foreign_server->serverid;
	*servers = lappend(*servers, hypertable_server);

	return true;
}

List *
hypertable_server_scan(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	List *hypertable_servers = NIL;

	ScanKeyInit(&scankey[0], Anum_hypertable_server_hypertable_id_server_name_idx_hypertable_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(hypertable_id));

	hypertable_server_scan_limit_internal(scankey,
										  1,
										  HYPERTABLE_SERVER_HYPERTABLE_ID_SERVER_NAME_IDX,
										  hypertable_server_tuple_found,
										  &hypertable_servers,
										  0,
										  AccessShareLock);

	return hypertable_servers;
}
