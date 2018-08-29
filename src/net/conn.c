#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn_internal.h"

static ConnOps *conn_ops[_CONNECTION_MAX] = { NULL };

static const char *conn_names[] = {
	[CONNECTION_PLAIN] = "PLAIN",
	[CONNECTION_SSL] = "SSL",
	[CONNECTION_MOCK] = "MOCK",
};

static Connection *
connection_internal_create(ConnectionType type, ConnOps *ops)
{
	Connection *conn = palloc(ops->size);

	if (NULL == conn)
		return NULL;

	memset(conn, 0, ops->size);
	conn->ops = ops;
	conn->type = type;

	return conn;
}

Connection *
connection_create(ConnectionType type)
{
	Connection *conn;

	if (type == _CONNECTION_MAX)
		elog(ERROR, "connection library: invalid connection type");

	if (NULL == conn_ops[type])
			ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("connection library: %s connections are not supported", conn_names[type]),
			 errhint("Enable %s support when compiling the extension.", conn_names[type])));

	conn = connection_internal_create(type, conn_ops[type]);

	Assert(NULL != conn);

	if (NULL != conn->ops->init)
		if (conn->ops->init(conn) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("connection library: %s connection could not be initialized",
							conn_names[type])));

	return conn;
}

int
connection_connect(Connection *conn, const char *host, int port)
{
	return conn->ops->connect(conn, host, port);
}

ssize_t
connection_write(Connection *conn, const char *buf, size_t writelen)
{
	int			bytes;

	bytes = conn->ops->write(conn, buf, writelen);

	if (bytes <= 0 || bytes != writelen)
		elog(ERROR, "connection library: could not write");

	return bytes;
}

ssize_t
connection_read(Connection *conn, char *buf, size_t buflen)
{
	return conn->ops->read(conn, buf, buflen);
}

void
connection_close(Connection *conn)
{
	if (NULL != conn->ops)
		conn->ops->close(conn);
}

void
connection_destroy(Connection *conn)
{
	if (conn == NULL)
		return;

	connection_close(conn);
	conn->ops = NULL;
	pfree(conn);
}

void
connection_register(ConnectionType type, ConnOps *ops)
{
	if (type == _CONNECTION_MAX)
		elog(ERROR, "invalid connection type");

	conn_ops[type] = ops;
}
