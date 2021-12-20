/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "scan_iterator.h"

TSDLLEXPORT void
ts_scan_iterator_set_index(ScanIterator *iterator, CatalogTable table, int indexid)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), table, indexid);
}

void
ts_scan_iterator_close(ScanIterator *iterator)
{
	ts_scanner_end_and_close_scan(&iterator->ctx, &iterator->ictx);
}

TSDLLEXPORT void
ts_scan_iterator_scan_key_init(ScanIterator *iterator, AttrNumber attributeNumber,
							   StrategyNumber strategy, RegProcedure procedure, Datum argument)
{
	MemoryContext oldmcxt;

	Assert(iterator->ctx.scankey == NULL || iterator->ctx.scankey == iterator->scankey);
	iterator->ctx.scankey = iterator->scankey;
	
	if (iterator->ctx.nkeys >= EMBEDDED_SCAN_KEY_SIZE)
		elog(ERROR, "cannot scan more than %d keys", EMBEDDED_SCAN_KEY_SIZE);


	/* For rescans, when the scan key is reinitialized during the scan, make
	 * sure the scan eky is initialized on the long-lived scankey memory
	 * context. */ 
	oldmcxt = MemoryContextSwitchTo(iterator->scankey_mcxt);
	ScanKeyInit(&iterator->scankey[iterator->ctx.nkeys++],
				attributeNumber,
				strategy,
				procedure,
				argument);
    MemoryContextSwitchTo(oldmcxt);
}

TSDLLEXPORT void
ts_scan_iterator_rescan(ScanIterator *iterator)
{
	ts_scanner_rescan(&iterator->ctx, &iterator->ictx, NULL);
}
