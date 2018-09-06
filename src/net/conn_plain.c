#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <pg_config.h>

#include "conn_internal.h"
#include "conn_plain.h"

#define DEFAULT_TIMEOUT_SEC	3
#define MAX_PORT 65535

static void
set_error(int err)
{
#ifdef WIN32
	WSASetLastError(err);
#else
	errno = err;
#endif
}

/*  Create socket and connect */
int
plain_connect(Connection *conn, const char *host, const char *servname, int port)
{
	char		strport[6];
	struct addrinfo *ainfo,
				hints = {
		.ai_family = PF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
	};
	struct timeval timeouts = {
		.tv_sec = DEFAULT_TIMEOUT_SEC,
	};
	int			ret;

	if (NULL == servname && (port <= 0 || port > MAX_PORT))
	{
		set_error(EINVAL);
		return -1;
	}

	/* Explicit port given. Use it instead of servname */
	if (port > 0 && port <= MAX_PORT)
	{
		snprintf(strport, sizeof(strport), "%d", port);
		servname = strport;
		hints.ai_flags = AI_NUMERICSERV;
	}

	/* Lookup the endpoint ip address */
	ret = getaddrinfo(host, servname, &hints, &ainfo);

	if (ret < 0)
		goto out;

#ifdef WIN32
	conn->sock = WSASocket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol, NULL, 0, 0);

	if (conn->sock == INVALID_SOCKET)
		ret = -1;
#else
	ret = conn->sock = socket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol);

#endif

	if (ret < 0)
		goto out_addrinfo;

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	ret = setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeouts, sizeof(struct timeval));

	if (ret < 0)
		goto out_addrinfo;

	ret = setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeouts, sizeof(struct timeval));

	if (ret < 0)
		goto out_addrinfo;

	/* connect the socket */
	ret = connect(conn->sock, ainfo->ai_addr, ainfo->ai_addrlen);

out_addrinfo:
	freeaddrinfo(ainfo);

out:
	if (ret < 0)
		conn->err = ret;

	return ret;
}

static ssize_t
plain_write(Connection *conn, const char *buf, size_t writelen)
{
	int			ret = send(conn->sock, buf, writelen, 0);

	if (ret < 0)
		conn->err = ret;

	return ret;
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	int			ret = recv(conn->sock, buf, buflen, 0);

	if (ret < 0)
		conn->err = ret;

	return ret;
}

void
plain_close(Connection *conn)
{
#ifdef WIN32
	closesocket(conn->sock);
#else
	close(conn->sock);
#endif
}

static const char *
plain_errmsg(Connection *conn)
{
	const char *errmsg = "no connection error";
#ifdef WIN32
	int			error = WSAGetLastError();
#else
	int			error = errno;
#endif

	if (conn->err == 0)
		return errmsg;

	if (conn->err < 0)
		errmsg = strerror(error);

	conn->err = 0;

	return "unknown connection error";
}

static ConnOps plain_ops = {
	.size = sizeof(Connection),
	.init = NULL,
	.connect = plain_connect,
	.close = plain_close,
	.write = plain_write,
	.read = plain_read,
	.errmsg = plain_errmsg,
};

extern void _conn_plain_init(void);
extern void _conn_plain_fini(void);

void
_conn_plain_init(void)
{
#ifdef WIN32
	WSADATA		wsadata;
	int			res;

	/* Probably called by Postmaster already, but might as well redo */
	res = WSAStartup(MAKEWORD(2, 2), &wsadata);

	if (res != 0)
	{
		elog(ERROR, "WSAStartup failed: %d", res);
		return;
	}
#endif
	connection_register(CONNECTION_PLAIN, &plain_ops);
}

void
_conn_plain_fini(void)
{
#ifdef WIN32
	int			ret = WSACleanup();

	if (ret != 0)
		elog(WARNING, "WSACleanup failed");
#endif
}
