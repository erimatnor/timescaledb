#include <postgres.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <pg_config.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "conn_internal.h"
#include "conn_plain.h"

typedef struct SSLConnection
{
	Connection	conn;
	SSL_CTX    *ssl_ctx;
	SSL		   *ssl;
	unsigned long errcode;
} SSLConnection;

static void
ssl_set_error(SSLConnection *conn, int err)
{
	conn->errcode = ERR_get_error();
	conn->conn.err = err;
}

static int
ssl_setup(SSLConnection *conn)
{
	int			ret;

	conn->ssl_ctx = SSL_CTX_new(TLSv1_2_method());

	if (NULL == conn->ssl_ctx)
	{
		ssl_set_error(conn, -1);
		return -1;
	}

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
		ssl_set_error(conn, -1);
		return -1;
	}

	ERR_clear_error();

	ret = SSL_set_fd(conn->ssl, conn->conn.sock);

	if (ret == 0)
	{
		ssl_set_error(conn, -1);
		return -1;
	}

	ret = SSL_connect(conn->ssl);

	if (ret <= 0)
	{
		ssl_set_error(conn, ret);
		ret = -1;
	}

	return ret;
}

static int
ssl_connect(Connection *conn, const char *host, const char *servname, int port)
{
	int			ret;

	/* First do the base connection setup */
	ret = plain_connect(conn, host, servname, port);

	if (ret < 0)
		return ret;

	return ssl_setup((SSLConnection *) conn);
}

static ssize_t
ssl_write(Connection *conn, const char *buf, size_t writelen)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	int			ret = SSL_write(sslconn->ssl, buf, writelen);

	if (ret < 0)
		ssl_set_error(sslconn, ret);

	return ret;
}

static ssize_t
ssl_read(Connection *conn, char *buf, size_t buflen)
{
	SSLConnection *sslconn = (SSLConnection *) conn;

	int			ret = SSL_read(sslconn->ssl, buf, buflen);

	if (ret < 0)
		ssl_set_error(sslconn, ret);

	return ret;
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
ssl_errmsg(Connection *conn)
{
	SSLConnection *sslconn = (SSLConnection *) conn;
	const char *reason;
	static char errbuf[32];
	int			err = conn->err;
	unsigned long ecode = sslconn->errcode;

	/* Clear errors */
	conn->err = 0;
	sslconn->errcode = 0;

	if (NULL != sslconn->ssl)
	{
		switch (SSL_get_error(sslconn->ssl, err))
		{
			case SSL_ERROR_NONE:
			case SSL_ERROR_SSL:
				break;
			case SSL_ERROR_ZERO_RETURN:
				return "SSL error zero return";
			case SSL_ERROR_WANT_READ:
				return "SSL error want read";
			case SSL_ERROR_WANT_WRITE:
				return "SSL error want write";
			case SSL_ERROR_WANT_CONNECT:
				return "SSL error want connect";
			case SSL_ERROR_WANT_ACCEPT:
				return "SSL error want accept";
			case SSL_ERROR_WANT_X509_LOOKUP:
				return "SSL error want X509 lookup";
			case SSL_ERROR_SYSCALL:
				if (ecode == 0)
				{
					if (err == 0)
						return "EOF in SSL operation";
					else if (err == -1)
					{
#ifdef WIN32
						int			syserr = WSAGetLastError();
#else
						int			syserr = errno;
#endif
						snprintf(errbuf, sizeof(errbuf), "SSL I/O error: %s", strerror(syserr));
						return errbuf;
					}
					else
						return "unknown SSL syscall error";
				}
				return "SSL error syscall";
			default:
				break;
		}
	}

	if (ecode == 0)
		return "no SSL error";

	reason = ERR_reason_error_string(ecode);

	if (NULL != reason)
		return reason;

	snprintf(errbuf, sizeof(errbuf), "SSL error code %lu", ecode);

	return errbuf;
}

static ConnOps ssl_ops = {
	.size = sizeof(SSLConnection),
	.init = NULL,
	.connect = ssl_connect,
	.close = ssl_close,
	.write = ssl_write,
	.read = ssl_read,
	.errmsg = ssl_errmsg,
};

extern void _conn_ssl_init(void);
extern void _conn_ssl_fini(void);

void
_conn_ssl_init(void)
{
	SSL_library_init();
	/* Always returns 1 */
	SSL_load_error_strings();
	connection_register(CONNECTION_SSL, &ssl_ops);
}

void
_conn_ssl_fini(void)
{
	ERR_free_strings();
}
