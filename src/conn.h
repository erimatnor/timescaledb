#include <stdio.h>
#include <stdlib.h>
#include <pg_config.h>

#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

#define ENABLE_MOCK_CONN 1

typedef struct ConnOps ConnOps;

typedef struct Connection
{
	int			sock;
	ConnOps    *ops;
	unsigned long errcode;
} Connection;

#if defined(ENABLE_MOCK_CONN)
extern Connection *connection_create_mock(void);
extern const char *connection_mock_get_sent_data(Connection *conn, size_t *datalen);
extern ssize_t connection_mock_set_recv_data(Connection *conn, const char *data, size_t datalen);
#endif

extern Connection *connection_create_ssl(void);
extern Connection *connection_create_plain(void);
extern int	connection_connect(Connection *conn, const char *host, int port);
extern ssize_t connection_read(Connection *conn, char *buf, size_t buflen);
extern ssize_t connection_write(Connection *conn, const char *buf, size_t writelen);
extern int	connection_close(Connection *conn);
extern void connection_destroy(Connection *conn);
extern const char *connection_err_msg(Connection *conn);

/*  Called in init.c */
extern void _connection_init(void);
extern void _connection_fini(void);
