#ifndef TIMESCALEDB_NET_CONN_PLAIN_H
#define TIMESCALEDB_NET_CONN_PLAIN_H

typedef struct Connection Connection;

extern int	plain_connect(Connection *conn, const char *host, int port);
extern void plain_close(Connection *conn);

#endif							/* TIMESCALEDB_NET_CONN_PLAIN_H */
