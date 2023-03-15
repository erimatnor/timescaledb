/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_LIBPQ_LIBPQ_INT_H
#define TIMESCALEDB_TSL_REMOTE_LIBPQ_LIBPQ_INT_H

#include <compat/compat.h>

#if PG12
#include "libpq-int-pg12.h"
#elif PG13
#include "libpq-int-pg13.h"
#elif PG14
#include "libpq-int-pg14.h"
#elif PG15
#include "libpq-int-pg15.h"
#else
#error "libpq-int.h not imported for this PostgreSQL version"
#endif

#endif /* TIMESCALEDB_TSL_REMOTE_LIBPQ_LIBPQ_INT_H */
