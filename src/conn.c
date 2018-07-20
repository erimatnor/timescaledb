#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn.h"

typedef struct ConnOps
{
	int			(*connect) (Connection *conn, const char *host, int port);
	int			(*close) (Connection *conn);
	ssize_t		(*write) (Connection *conn, const char *buf, size_t writelen);
	ssize_t		(*read) (Connection *conn, char *buf, size_t readlen);
	const char *(*err_msg) (Connection *conn);
} ConnOps;


/*  Create socket and connect */
static int
plain_connect(Connection *conn, const char *host, int port)
{
	struct addrinfo *server_ip;
	struct sockaddr_in serv_info;
	struct sockaddr_in *temp;
	struct timeval timeouts = {
		.tv_sec = 3,
	};

	conn->sock = socket(AF_INET, SOCK_STREAM, 0);

	if (conn->sock < 0)
		return conn->sock;

	/*
	 * Set send / recv timeout so that write and read don't block forever
	 */
	if (setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, &timeouts, sizeof(struct timeval)) != 0 ||
		setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, &timeouts, sizeof(struct timeval)) != 0)
		elog(LOG, "connection library: could not set timeouts on SSL sockets");

	/* lookup the ip address */
	if (getaddrinfo(host, NULL, NULL, &server_ip) < 0 || server_ip == NULL)
	{
		elog(LOG, "connection library: could not get IP of endpoint");
		close(conn->sock);
		return -1;
	}
	memset(&serv_info, 0, sizeof(serv_info));
	serv_info.sin_family = AF_INET;
	serv_info.sin_port = htons(port);
	temp = (struct sockaddr_in *) (server_ip->ai_addr);

	memcpy(&serv_info.sin_addr.s_addr, &(temp->sin_addr.s_addr), sizeof(serv_info.sin_addr.s_addr));

	freeaddrinfo(server_ip);

	/* connect the socket */
	if (connect(conn->sock, (struct sockaddr *) &serv_info, sizeof(serv_info)) < 0)
	{
		elog(LOG, "connection library: could not connect to endpoint");
		close(conn->sock);
		return -1;
	}

	return 0;
}

static ssize_t
plain_write(Connection *conn, const char *buf, size_t writelen)
{
	return write(conn->sock, buf, writelen);
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	return read(conn->sock, buf, buflen);
}

static int
plain_close(Connection *conn)
{
	return close(conn->sock);
}

static const char *
plain_err_msg(Connection *conn)
{
	return strerror(conn->errcode);
}

static ConnOps plain_ops = {
	.connect = plain_connect,
	.close = plain_close,
	.write = plain_write,
	.read = plain_read,
	.err_msg = plain_err_msg,
};

static Connection *
connection_create(size_t size, ConnOps *ops)
{
	Connection *conn = malloc(size);

	if (NULL == conn)
		return NULL;

	memset(conn, 0, sizeof(*conn));
	conn->ops = ops;

	return conn;
}

Connection *
connection_create_plain()
{
	return connection_create(sizeof(Connection), &plain_ops);
}

#ifdef USE_OPENSSL

typedef struct SSLConnection
{
	Connection	conn;
	SSL_CTX    *ssl_ctx;
	SSL		   *ssl;
} SSLConnection;


static int
ssl_setup(SSLConnection *conn)
{
	conn->ssl_ctx = SSL_CTX_new(SSLv23_method());

	if (NULL == conn->ssl_ctx)
	{
		elog(LOG, "connection library: could not create SSL context");
		conn->conn.errcode = ERR_get_error();
		return -1;
	}

	SSL_CTX_set_options(conn->ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

	/*
	 * Because we have a blocking socket, we don't want to be bothered with
	 * retries.
	 */
	SSL_CTX_set_mode(conn->ssl_ctx, SSL_MODE_AUTO_RETRY);

	ERR_clear_error();
	/* Clear the SSL error before next SSL_ * call */
	conn->ssl = SSL_new(conn->ssl_ctx);

	if (conn->ssl == NULL)
	{
		elog(LOG, "connection library: could not create SSL connection");
		SSL_CTX_free(conn->ssl_ctx);
		conn->ssl_ctx = NULL;
		conn->conn.errcode = ERR_get_error();
		return -1;
	}
	ERR_clear_error();

	if (SSL_set_fd(conn->ssl, conn->conn.sock) == 0)
	{
		elog(LOG, "connection library: could not associate socket \
			with SSL connection");
		goto err;
	}

	if (SSL_connect(conn->ssl) <= 0)
	{
		elog(LOG, "connection library: could not make SSL connection");
		goto err;
	}

	return 0;
err:
	conn->conn.errcode = ERR_get_error();
	SSL_free(conn->ssl);
	SSL_CTX_free(conn->ssl_ctx);
	conn->ssl = NULL;
	conn->ssl_ctx = NULL;

	return -1;
}

static int
ssl_connect(Connection *conn, const char *host, int port)
{
	int			ret;

	/* First do the base connection setup */
	ret = plain_connect(conn, host, port);

	if (ret < 0)
		return ret;

	ret = ssl_setup((SSLConnection *) conn);

	if (ret < 0)
		close(conn->sock);

	return ret;
}

static ssize_t
ssl_write(Connection *conn, const char *buf, size_t writelen)
{
	elog(LOG, "about to SSL_write req");

	return SSL_write(((SSLConnection *) conn)->ssl, buf, writelen);
}

static ssize_t
ssl_read(Connection *conn, char *buf, size_t buflen)
{
	return SSL_read(((SSLConnection *) conn)->ssl, buf, buflen);
}

static int
ssl_close(Connection *conn)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	if (sslconn->ssl != NULL)
	{
		SSL_free(sslconn->ssl);
		sslconn->ssl = NULL;
	}

	if (sslconn->ssl_ctx != NULL)
	{
		SSL_CTX_free(sslconn->ssl_ctx);
		sslconn->ssl_ctx = NULL;
	}

	return plain_close(conn);
}

static const char *
ssl_err_msg(Connection *conn)
{
	return ERR_reason_error_string(conn->errcode);
}

static ConnOps ssl_ops = {
	.connect = ssl_connect,
	.close = ssl_close,
	.write = ssl_write,
	.read = ssl_read,
	.err_msg = ssl_err_msg,
};

#endif							/* USE_OPENSSL */


Connection *
connection_create_ssl()
{
#ifdef USE_OPENSSL
	return connection_create(sizeof(SSLConnection), &ssl_ops);
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("SSL connections are not supported"),
			 errhint("Enable SSL support when compiling the extension.")));
#endif
}


/* Public API */

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
	{
		elog(LOG, "connection library: could not_write");
		return -1;
	}

	return bytes;
}

ssize_t
connection_read(Connection *conn, char *buf, size_t buflen)
{
	int			bytes;
	int			offset = 0;

	while (buflen > 0)
	{
		bytes = conn->ops->read(conn, buf + offset, buflen);

		if (bytes == 0)
			break;

		if (bytes < 0)
		{
			elog(LOG, "connection library: could not read");
			return -1;
		}
		offset += bytes;
		buflen -= bytes;
	}

	return offset;
}

int
connection_close(Connection *conn)
{
	if (NULL != conn->ops)
		return conn->ops->close(conn);

	return 0;
}

void
connection_destroy(Connection *conn)
{
	if (conn == NULL)
		return;

	connection_close(conn);
	conn->ops = NULL;
	free(conn);
}

const char *
connection_err_msg(Connection *conn)
{
	return conn->ops->err_msg(conn);
}

void
_connection_init(void)
{
#ifdef USE_OPENSSL
	SSL_library_init();
	/* Always returns 1 */
	SSL_load_error_strings();
#endif
}

void
_connection_fini(void)
{
#ifdef USE_OPENSSL
	ERR_free_strings();
#endif
}


#if defined(ENABLE_MOCK_CONN)

/*
 * States for the mocked connection.
 */
typedef enum MockConnState
{
	MOCK_CONN_INIT,
	MOCK_CONN_CONNECTED,
	MOCK_CONN_DATA_SENT,
	MOCK_CONN_ERROR,
} MockConnState;

#define MAX_MSG_LEN 2048

/*
 * Mocked connection.
 *
 * Provides a send and recv buffer.
 */
typedef struct MockConnection
{
	Connection	conn;
	MockConnState state;
	size_t		sendbuf_written;
	size_t		recvbuf_written;
	size_t		recvbuf_read;
	char		sendbuf[MAX_MSG_LEN];
	char		recvbuf[MAX_MSG_LEN];
} MockConnection;

static int
mock_connect(Connection *conn, const char *host, int port)
{
	MockConnection *mock = (MockConnection *) conn;

	if (mock->state >= MOCK_CONN_CONNECTED)
	{
		conn->errcode = EISCONN;
		return -1;
	}

	mock->state = MOCK_CONN_CONNECTED;

	return 0;
}

static int
mock_close(Connection *conn)
{
	MockConnection *mock = (MockConnection *) conn;

	if (mock->state < MOCK_CONN_CONNECTED)
	{
		conn->errcode = EBADF;
		return -1;
	}

	mock->state = MOCK_CONN_INIT;
	mock->sendbuf_written = 0;
	mock->recvbuf_written = 0;
	mock->recvbuf_read = 0;

	return 0;
}

static ssize_t
mock_write(Connection *conn, const char *buf, size_t writelen)
{
	MockConnection *mock = (MockConnection *) conn;

	switch (mock->state)
	{
		case MOCK_CONN_CONNECTED:
			if (writelen > MAX_MSG_LEN)
			{
				conn->errcode = ENOBUFS;
				return -1;
			}
			mock->state = MOCK_CONN_DATA_SENT;

			/* Only "send" half the buffer the first write */
			mock->sendbuf_written = writelen / 2;
			memcpy(mock->sendbuf, buf, mock->sendbuf_written);

			return mock->sendbuf_written;
		case MOCK_CONN_DATA_SENT:
			/* Send the rest or buffer full */
			if (writelen > (MAX_MSG_LEN - mock->sendbuf_written))
			{
				conn->errcode = ENOBUFS;
				return -1;
			}

			memcpy(mock->sendbuf + mock->sendbuf_written, buf, writelen);
			return writelen;
		case MOCK_CONN_ERROR:
			return -1;
		default:
			conn->errcode = ECONNRESET;
			mock->state = MOCK_CONN_ERROR;
			return -1;
	}

	return writelen;
}

static ssize_t
mock_read(Connection *conn, char *buf, size_t readlen)
{
	MockConnection *mock = (MockConnection *) conn;

	switch (mock->state)
	{
		case MOCK_CONN_CONNECTED:
			conn->errcode = EAGAIN;
			return -1;
		case MOCK_CONN_DATA_SENT:
			if (readlen > (mock->recvbuf_written - mock->recvbuf_read))
			{
				/* Pretend non-blocking mode */
				conn->errcode = EWOULDBLOCK;
				return -1;
			}

			memcpy(buf, mock->recvbuf + mock->recvbuf_read, readlen);
			mock->recvbuf_read += readlen;
			return readlen;
		case MOCK_CONN_ERROR:
			return -1;
		default:
			conn->errcode = ECONNRESET;
			mock->state = MOCK_CONN_ERROR;
			return -1;
	}

	return 0;
}

static ConnOps mock_ops = {
	.connect = mock_connect,
	.close = mock_close,
	.write = mock_write,
	.read = mock_read,
	.err_msg = plain_err_msg,
};

ssize_t
connection_mock_set_recv_data(Connection *conn, const char *data, size_t datalen)
{
	MockConnection *mock = (MockConnection *) conn;

	if (datalen > MAX_MSG_LEN)
		return -1;

	memcpy(mock->recvbuf, data, datalen);

	mock->recvbuf_written = datalen;
	mock->recvbuf_read = 0;

	return datalen;
}

const char *
connection_mock_get_sent_data(Connection *conn, size_t *datalen)
{
	MockConnection *mock = (MockConnection *) conn;

	if (NULL != datalen)
		*datalen = mock->sendbuf_written;

	return mock->sendbuf;
}

Connection *
connection_create_mock()
{
	return connection_create(sizeof(MockConnection), &mock_ops);
}

#endif							/* ENABLE_MOCK_CONN */
