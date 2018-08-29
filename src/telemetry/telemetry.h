#ifndef TIMESCALEDB_TELEMETRY_TELEMETRY_H
#define TIMESCALEDB_TELEMETRY_TELEMETRY_H
#include <postgres.h>
#include <fmgr.h>
#include <pg_config.h> // To get USE_OPENSSL from postgres build
#include <utils/builtins.h>

#include "compat.h"
#include "guc.h"
#include "version.h"
#include "net/conn.h"
#include "net/http.h"
#include "net/uri.h"
#include "utils.h"

HttpRequest *build_version_request(const char *host, const char *path);
Connection *telemetry_connect(URI *uri);

/*
 *	This function is intended as the main function for a BGW.
 *  Its job is to send metrics and fetch the most up-to-date version of
 *  Timescale via HTTPS.
 */
void		telemetry_main(void);

#endif							/* TIMESCALEDB_TELEMETRY_TELEMETRY_H */
