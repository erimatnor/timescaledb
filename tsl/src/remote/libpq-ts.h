/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_LIBPQ_TS_H
#define TIMESCALEDB_TSL_REMOTE_LIBPQ_TS_H

#include <libpq-fe.h>

extern int ts_grow_output_buffer(PGconn *conn, size_t len);
extern int ts_check_out_buffer_space(size_t bytes_needed, PGconn *conn);
extern int ts_put_msg_start(char msg_type, PGconn *conn);
extern int ts_putnchar(const char *s, size_t len, PGconn *conn);
extern int ts_put_msg_end(PGconn *conn);

#endif /* TIMESCALEDB_TSL_REMOTE_LIBPQ_TS_H */
