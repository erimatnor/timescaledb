#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/datum.h>

#include "catalog.h"
#include "installation_metadata.h"
#include "scanner.h"

static Datum
convert_value_to_text(Datum value, Oid from_type)
{
	bool value_is_varlena;
	Oid value_out;

	getTypeOutputInfo(from_type, &value_out, &value_is_varlena);

	if (!OidIsValid(value_out))
		elog(ERROR, "no output function for type %u", from_type);

	return DirectFunctionCall1(textin, OidFunctionCall1(value_out, value));
}

static Datum
convert_key_to_name(Datum value, Oid from_type)
{
	bool value_is_varlena;
	Oid value_out;

	getTypeOutputInfo(from_type, &value_out, &value_is_varlena);

	if (!OidIsValid(value_out))
		elog(ERROR, "no output function for type %u", from_type);

	return DirectFunctionCall1(namein, OidFunctionCall1(value_out, value));
}

static Datum
convert_text_to_value(Datum value, Oid to_type)
{
	Oid value_in;
	Oid value_ioparam;

	getTypeInputInfo(to_type, &value_in, &value_ioparam);

	if (!OidIsValid(value_in))
		elog(ERROR, "no input function for value type %u", to_type);

	value = OidFunctionCall3(value_in,
							 CStringGetDatum(TextDatumGetCString(value)),
							 ObjectIdGetDatum(InvalidOid),
							 Int32GetDatum(-1));
	return value;
}

typedef struct DatumValue
{
	Datum value;
	Oid  typeid;
	bool isnull;
} DatumValue;

static bool
installation_metadata_tuple_get_value(TupleInfo *ti, void *data) {
	DatumValue *dv = data;

	dv->value = heap_getattr(ti->tuple, Anum_installation_metadata_value, ti->desc, &dv->isnull);

	if (!dv->isnull)
		dv->value = convert_text_to_value(dv->value, dv->typeid);

	return false;
}

static Datum
installation_metadata_get_value_internal(Datum metadata_key,
										 Oid key_type,
										 Oid value_type,
										 bool *isnull,
										 LOCKMODE lockmode)
{
	Oid key_out;
	bool key_is_varlena;
	ScanKeyData scankey[1];
	DatumValue dv = {
		.typeid = value_type,
		.isnull = true,
	};
	Catalog    *catalog = catalog_get();
	ScannerCtx scanctx = {
		.table = catalog->tables[INSTALLATION_METADATA].id,
		.index = CATALOG_INDEX(catalog, INSTALLATION_METADATA, INSTALLATION_METADATA_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = installation_metadata_tuple_get_value,
		.data = &dv,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	getTypeOutputInfo(key_type, &key_out, &key_is_varlena);

	if (!OidIsValid(key_out))
		elog(ERROR, "no output function for key type %u", key_type);

	ScanKeyInit(&scankey[0], Anum_installation_metadata_key, BTEqualStrategyNumber, F_NAMEEQ,
				DirectFunctionCall1(namein, OidFunctionCall1(key_out, metadata_key)));

	scanner_scan(&scanctx);

	if (NULL != isnull)
		*isnull = dv.isnull;

	return dv.value;
}

Datum
installation_metadata_get_value(Datum metadata_key,
								Oid key_type,
								Oid value_type,
								bool *isnull)
{
	return installation_metadata_get_value_internal(metadata_key,
													key_type,
													value_type,
													isnull,
													AccessShareLock);
}

/*
 *  Insert a row into the installation_metadata table. Acquires a lock in SHARE mode, before
 *  verifying that the desired metadata KV pair still does not exist. Otherwise, exits without
 *  inserting to avoid underlying database error on PK conflict.
 *  Returns the value of the key; this is either the requested insert value or the
 *  existing value if nothing was inserted.
 */
Datum
installation_metadata_insert(Datum metadata_key, Oid key_type, Datum metadata_value, Oid value_type)
{
	Datum existing_value;
	Datum		values[Natts_installation_metadata];
	bool		nulls[Natts_installation_metadata] = {false};
	bool        isnull = false;
	Catalog    *catalog = catalog_get();
	Relation rel;

	rel = heap_open(catalog->tables[INSTALLATION_METADATA].id, ShareLock);

	/* Check for row existence while we have the lock */
	existing_value = installation_metadata_get_value_internal(metadata_key,
															  key_type,
															  value_type,
															  &isnull,
															  ShareLock);

	if (!isnull)
	{
		heap_close(rel, ShareLock);
		return existing_value;
	}

	/* Insert into the catalog table for persistence */
	values[AttrNumberGetAttrOffset(Anum_installation_metadata_key)] =
		convert_key_to_name(metadata_key, key_type);
	values[AttrNumberGetAttrOffset(Anum_installation_metadata_value)] =
		convert_value_to_text(metadata_value, value_type);

	catalog_insert_values(rel, RelationGetDescr(rel), values, nulls);

	heap_close(rel, ShareLock);

	return metadata_value;
}
