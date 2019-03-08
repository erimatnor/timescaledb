/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLANNER_H
#define TIMESCALEDB_PLANNER_H

#include <postgres.h>
#include <nodes/pg_list.h>

typedef struct TsFdwRelationInfo TsFdwRelationInfo;
typedef struct TimescaleDBPrivate
{
	bool appends_ordered;
	List *chunk_oids;
	TsFdwRelationInfo *fdw_relation_info;
} TimescaleDBPrivate;

#endif /* TIMESCALEDB_PLANNER_H */
