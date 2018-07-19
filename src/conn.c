#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn.h"

typedef struct ConnOps
{
	int			(*connect) (Connection *conn, const char *host, int port);
	void		(*close) (Connection *conn);
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

static void
plain_close(Connection *conn)
{
	close(conn->sock);
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

static void
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

	plain_close(conn);
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
	//Always returns 1
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
