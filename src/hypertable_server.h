#ifndef TIMESCALEDB_HYPERTABLE_SERVER_H
#define TIMESCALEDB_HYPERTABLE_SERVER_H

#include "catalog.h"

typedef struct HypertableServer {
	FormData_hypertable_server fd;
} HypertableServer;

extern List *hypertable_server_scan(int32 hypertable_id);

#endif /* TIMESCALEDB_HYPERTABLE_SERVER_H */
