/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern void ts_finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog,
								bool swap_toast_by_content, bool check_constraints,
								bool is_internal, TransactionId frozenXid, MultiXactId cutoffMulti,
								char newrelpersistence);
