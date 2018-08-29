#include <postgres.h>

#include "uri.h"

static const char *uri_scheme_names[] = {
	[URI_SCHEME_HTTP] = "http",
	[URI_SCHEME_HTTPS] = "https",
	[URI_SCHEME_INVALID] = "invalid",
};

typedef enum URIParseState
{
	PARSE_STATE_SCHEME,
	PARSE_STATE_AUTHORITY_SLASH1,
	PARSE_STATE_AUTHORITY_SLASH2,
	PARSE_STATE_HOST,
	PARSE_STATE_PORT,
	PARSE_STATE_PATH,
} URIParseState;

typedef struct URIParser
{
	const char *raw_uri;
	URIParseState prev_state, state;
	off_t tail, head;
	URIScheme scheme;
	const char *host;
	const char *path;
	int port;
	const char *error;
} URIParser;

const char *
uri_scheme(URI *uri)
{
	return uri_scheme_names[uri->scheme];
}

static URIScheme
parse_scheme(URIParser *parser)
{
	const char *scheme = &parser->raw_uri[parser->tail];
	int i;

	for (i = 0; i < sizeof(uri_scheme_names) / sizeof(char *); i++)
		if (strncmp(uri_scheme_names[i], scheme, parser->head - parser->tail) == 0)
			return i;

	return URI_SCHEME_INVALID;
}

static int
parse_port(URIParser *parser)
{
	const char *raw_port = &parser->raw_uri[parser->tail];
	int port = strtol(raw_port, NULL, 10);

	if (port <= 0)
		return -1;

	return port;
}

static const char *
parse_host(URIParser *parser)
{
	size_t len = parser->head - parser->tail + 1;
	char *host = palloc(len);

	StrNCpy(host, &parser->raw_uri[parser->tail], len);

	return host;
}

static const char *
parse_path(URIParser *parser)
{
	/* We'd like to keep the initial slash, so make extra room */
	size_t len = parser->head - parser->tail + 2;
	char *path = palloc(len);

	/* Include the preceeding slash (one char back from tail) */
	StrNCpy(path, &parser->raw_uri[parser->tail - 1], len);

	/* TODO: validate path */
	return path;
}

static bool
uri_validate(URIParser *parser)
{
	if (parser->scheme == URI_SCHEME_INVALID)
	{
		parser->error = "invalid URI scheme";
		return false;
	}

	if (parser->host == NULL || strlen(parser->host) == 0)
	{
		parser->error = "invalid URI host";
		return false;
	}

	if (parser->port < 0 || parser->port > INT16_MAX)
	{
		parser->error = "invalid URI port";
		return false;
	}

	return true;
}

static void
parser_set_state(URIParser *parser, URIParseState new_state)
{
	parser->prev_state = parser->state;
	parser->state = new_state;
	parser->tail = parser->head + 1;
}

static bool
parser_handle_colon(URIParser *parser)
{
	switch (parser->state)
	{
	case PARSE_STATE_SCHEME:
		Assert(parser->prev_state == PARSE_STATE_SCHEME);
		parser->scheme = parse_scheme(parser);
		parser_set_state(parser, PARSE_STATE_AUTHORITY_SLASH1);
		break;
	case PARSE_STATE_HOST:
		Assert(parser->prev_state == PARSE_STATE_AUTHORITY_SLASH2);
		parser->host = parse_host(parser);
		parser_set_state(parser, PARSE_STATE_PORT);
		break;
	default:
		return false;
	}

	return true;
}

static bool
parser_handle_slash(URIParser *parser)
{
	switch (parser->state)
	{
	case PARSE_STATE_AUTHORITY_SLASH1:
		Assert(parser->prev_state == PARSE_STATE_SCHEME);
		parser_set_state(parser, PARSE_STATE_AUTHORITY_SLASH2);
		break;
	case PARSE_STATE_AUTHORITY_SLASH2:
		Assert(parser->prev_state == PARSE_STATE_AUTHORITY_SLASH1);
		parser_set_state(parser, PARSE_STATE_HOST);
		break;
	case PARSE_STATE_HOST:
		Assert(parser->prev_state == PARSE_STATE_AUTHORITY_SLASH2);
		parser->host = parse_host(parser);
		parser_set_state(parser, PARSE_STATE_PATH);
		break;
	case PARSE_STATE_PORT:
		Assert(parser->prev_state == PARSE_STATE_HOST);
		parser->port = parse_port(parser);
		parser_set_state(parser, PARSE_STATE_PATH);
		break;
	case PARSE_STATE_PATH:
		/* TODO: check for supported path characters */
		break;
	default:
		return false;
	}
	return true;
}

static bool
parser_handle_questionmark(URIParser *parser)
{
	switch (parser->state)
	{
	case PARSE_STATE_HOST:
	case PARSE_STATE_PORT:
	case PARSE_STATE_PATH:
		parser->error = "URI does not support query parameters";
	default:
		return false;
	}
	return true;
}

static bool
parser_handle_end(URIParser *parser)
{
	switch (parser->state)
	{
	case PARSE_STATE_PORT:
		Assert(parser->prev_state == PARSE_STATE_HOST);
		parser->port = parse_port(parser);
		break;
	case PARSE_STATE_HOST:
		Assert(parser->prev_state == PARSE_STATE_AUTHORITY_SLASH2);
		parser->host = parse_host(parser);
		break;
	case PARSE_STATE_PATH:
		Assert(parser->prev_state == PARSE_STATE_HOST);
		parser->path = parse_path(parser);
		break;
	default:
		return false;
	}

	return true;
}

URI *
uri_parse(const char *raw_uri, const char **errhint)
{
	URIParser parser = {
		.raw_uri = raw_uri,
		.scheme = URI_SCHEME_INVALID,
		.prev_state = PARSE_STATE_SCHEME,
		.state = PARSE_STATE_SCHEME,
		.tail = 0,
		.head = 0,
	};
	bool res, done = false;
	URI *uri;

	while (!done)
	{
		switch (parser.raw_uri[parser.head])
		{
		case ':':
			res = parser_handle_colon(&parser);
			break;
		case '/':
			res = parser_handle_slash(&parser);
			break;
		case '?':
			res = parser_handle_questionmark(&parser);
			break;
		case '\0':
			res = parser_handle_end(&parser);
			done = true;
		default:
			/* Skip char */
			break;
		}

		if (!res)
		{
			if (NULL == parser.error)
				parser.error = "invalid URI";
			goto error;
		}

		parser.head++;
	}

	if (!uri_validate(&parser))
		goto error;

	uri = palloc0(sizeof(URI));
	uri->scheme = parser.scheme;
	uri->authority.host = parser.host;
	uri->authority.port = parser.port;
	uri->path = parser.path;

	/* Set default port for given scheme if no explicit port given */
	if (parser.port == 0)
	{
		switch (parser.scheme)
		{
		case URI_SCHEME_HTTP:
			uri->authority.port = 80;
			break;
		case URI_SCHEME_HTTPS:
			uri->authority.port = 443;
			break;
		default:
			Assert(false);
			break;
		}
	}

	return uri;
error:
	if (NULL != errhint)
		*errhint = parser.error;

	return NULL;
}
