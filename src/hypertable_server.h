#ifndef TIMESCALEDB_HYPERTABLE_SERVER_H
#define TIMESCALEDB_HYPERTABLE_SERVER_H

#include "catalog.h"

typedef struct HypertableServer {
	FormData_hypertable_server fd;
} HypertableServer;

extern List *hypertable_server_scan(int32 hypertable_id);
extern void hypertable_server_insert_multi(List *hypertable_servers);

#endif /* TIMESCALEDB_HYPERTABLE_SERVER_H */
