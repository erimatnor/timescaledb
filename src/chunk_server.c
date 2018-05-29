#include "chunk_server.h"
#include "scanner.h"

static void
chunk_server_insert_relation(Relation rel,
								  int32 chunk_id,
								  int32 server_chunk_id,
								  Name server_name)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_chunk_server];
	bool		nulls[Natts_chunk_server] = {false};
	CatalogSecurityContext sec_ctx;

	values[Anum_chunk_server_chunk_id - 1] = Int32GetDatum(chunk_id);
	values[Anum_chunk_server_server_chunk_id - 1] = Int32GetDatum(server_chunk_id);
	values[Anum_chunk_server_server_name - 1] = NameGetDatum(server_name);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);
}

static void
chunk_server_insert(int32 chunk_id,
						 int32 server_chunk_id,
						 Name server_name)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;

	rel = heap_open(catalog->tables[CHUNK_SERVER].id, RowExclusiveLock);
	chunk_server_insert_relation(rel,
									  chunk_id,
									  server_chunk_id,
									  server_name);
	heap_close(rel, RowExclusiveLock);
}

static int
chunk_server_scan_limit_internal(ScanKeyData *scankey,
									  int num_scankeys,
									  int indexid,
									  tuple_found_func on_tuple_found,
									  void *scandata,
									  int limit,
									  LOCKMODE lock)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[CHUNK_SERVER].id,
		.index = CATALOG_INDEX(catalog, CHUNK_SERVER, indexid),
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
chunk_server_tuple_found(TupleInfo *ti, void *data)
{
	List **servers = data;
	Form_chunk_server form = (Form_chunk_server) GETSTRUCT(ti->tuple);
	ChunkServer *chunk_server = palloc(sizeof(ChunkServer));

	memcpy(&chunk_server->fd, form, sizeof(FormData_chunk_server));
	*servers = lappend(*servers, chunk_server);

	return true;
}

List *
chunk_server_scan(int32 chunk_id)
{
	ScanKeyData scankey[1];
	List *chunk_servers = NIL;

	ScanKeyInit(&scankey[0], Anum_chunk_server_chunk_id_server_name_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(chunk_id));

	chunk_server_scan_limit_internal(scankey,
									 1,
									 CHUNK_SERVER_CHUNK_ID_SERVER_NAME_IDX,
									 chunk_server_tuple_found,
									 &chunk_servers,
									 0,
									 AccessShareLock);

	return chunk_servers;
}
