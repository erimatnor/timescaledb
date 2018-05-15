#ifndef _TIMESCALEDB_SERVER_H
#define _TIMESCALEDB_SERVER_H

#include "catalog.h"

typedef struct Server
{
} Server;

extern Server *server_get_by_name(const char *server_name);

#endif /* _TIMESCALEDB_SERVER_H */
