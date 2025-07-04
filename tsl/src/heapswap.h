/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pg_list.h>
#include <utils/rel.h>

extern void ts_finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap, bool is_system_catalog,
								bool swap_toast_by_content, bool check_constraints,
								bool is_internal, bool reindex, TransactionId frozenXid,
								MultiXactId cutoffMulti, char newrelpersistence);
extern List *ts_build_new_indexes(Relation NewHeap, Relation OldHeap, List *OldIndexes);
extern void ts_swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
								   bool swap_toast_by_content, bool is_internal,
								   TransactionId frozenXid, MultiXactId cutoffMulti,
								   Oid *mapped_tables);
