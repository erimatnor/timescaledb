#include <postgres.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <funcapi.h>
#include <fmgr.h>

#include "net/uri.h"
#include "compat.h"

TS_FUNCTION_INFO_V1(test_parse_uri);

Datum
test_parse_uri(PG_FUNCTION_ARGS)
{
	const text *uritext = PG_GETARG_TEXT_P(0);
	TupleDesc tupdesc;
	URI *uri;
	Datum values[4];
	bool nulls[4] = { false };
	HeapTuple tuple;
	const char *errhint = NULL;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	uri = uri_parse(text_to_cstring(uritext), &errhint);

	if (NULL == uri)
		elog(ERROR, "%s", errhint);

	values[0] = CStringGetTextDatum(uri_scheme(uri));
	values[1] = CStringGetTextDatum(uri->authority.host);
	values[2] = Int32GetDatum(uri->authority.port);

	if (NULL == uri_path(uri))
		nulls[3] = true;
	else
		values[3] = CStringGetTextDatum(uri_path(uri));

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}
