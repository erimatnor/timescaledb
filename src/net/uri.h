#ifndef TIMESCALEDB_NET_URI_H
#define TIMESCALEDB_NET_URI_H

typedef enum URIScheme
{
	URI_SCHEME_HTTP,
	URI_SCHEME_HTTPS,
	URI_SCHEME_INVALID,
} URIScheme;

typedef struct Uri
{
	URIScheme scheme;
	struct {
		const char *host;
		int port;
	} authority;
	const char *path;
} URI;

#define uri_host(uri) (uri)->authority.host
#define uri_port(uri) (uri)->authority.port
#define uri_path(uri) (uri)->path

extern URI *uri_parse(const char *uri, const char **errhint);
extern const char *uri_scheme(URI *uri);

#endif /* TIMESCALEDB_NET_URI_H */
