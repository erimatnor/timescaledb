/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "invalidation_funcs.h"

#include <access/attnum.h>
#include <access/tupdesc.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <replication/logicalproto.h>

#include "compat/compat.h"
#include "cache.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"
#include "invalidation_plugin_cache.h"

TS_FUNCTION_INFO_V1(ts_invalidation_read_record);

enum
{
	Anum_invalidation_entry_hypertable_relid = 1,
	Anum_invalidation_entry_lowest_modified_value,
	Anum_invalidation_entry_greatest_modified_value,
	_Anum_invalidation_entry_max,
};

/*
 * Create a heap tuple from the invalidation record.
 */
static HeapTuple
invalidation_tuple_get_heap_tuple(const InvalidationCacheEntry *entry, TupleDesc tupdesc)
{
	Datum values[_Anum_invalidation_entry_max];
	bool nulls[_Anum_invalidation_entry_max] = { 0 };

	values[AttrNumberGetAttrOffset(Anum_invalidation_entry_hypertable_relid)] = ObjectIdGetDatum(entry->hypertable_relid);
	values[AttrNumberGetAttrOffset(Anum_invalidation_entry_lowest_modified_value)] = Int64GetDatum(entry->lowest_modified_value);
	values[AttrNumberGetAttrOffset(Anum_invalidation_entry_greatest_modified_value)] = Int64GetDatum(entry->greatest_modified_value);

	return heap_form_tuple(tupdesc, values, nulls);
}

/*
 * Read an encoded invalidation record coming from the plugin.
 *
 * Records from the plugin is coming in logical replication format, so decode
 * it and produce a single composite value in the same format as the
 * invalidation table.
 */
Datum
ts_invalidation_read_record(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	bytea *raw_record = PG_GETARG_BYTEA_P(0);
	StringInfoData info;
	InvalidationCacheEntry entry;
	
	initReadOnlyStringInfo(&info, VARDATA_ANY(raw_record), VARSIZE_ANY_EXHDR(raw_record));
	
	entry.hypertable_relid = pq_getmsgint32(&info);
	entry.lowest_modified_value = pq_getmsgint64(&info);
	entry.greatest_modified_value = pq_getmsgint64(&info);
	
	HeapTuple htup = invalidation_tuple_get_heap_tuple(&entry, tupdesc);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
