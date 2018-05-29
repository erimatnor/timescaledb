#ifndef TIMESCALEDB_CHUNK_SERVER_H
#define TIMESCALEDB_CHUNK_SERVER_H

#include "catalog.h"

typedef struct ChunkServer {
	FormData_chunk_server fd;
} ChunkServer;

extern List *chunk_server_scan(int32 chunk_id);

#endif /* TIMESCALEDB_CHUNK_SERVER_H */
