#ifndef _TIMESCALEDB_SERVER_H
#define _TIMESCALEDB_SERVER_H

#include "catalog.h"

typedef struct Server
{
} Server;

extern Server *server_get_by_name(const char *server_name);
extern List *server_get_list(void);
extern void server_exec_on_all(List *servers, const char *stmt);

#endif /* _TIMESCALEDB_SERVER_H */
