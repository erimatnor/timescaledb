#include <postgres.h>
#include <utils/timestamp.h>
#include <utils/backend_random.h>

#include "compat.h"
#include "telemetry/uuid.h"

/*
 * Generates a v4 UUID. Based on function pg_random_uuid() in the pgcrypto contrib module.
 *
 * Note that clib on Mac has a uuid_generate() function, so we call this uuid_create().
 */
pg_uuid_t *
uuid_create(void)
{
	TimestampTz ts;
	pg_uuid_t	*gen_uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));
	bool rand_success = pg_backend_random((char *) gen_uuid->data, UUID_LEN);

	/*
	 * If pg_backend_random cannot find sources of randomness, then we use the current
	 * timestamp as a "random source". Timestamps are 8 bytes, so we copy this into bytes 9-16 of the UUID.
	 * If we see all 0s in bytes 0-8 (other than version + variant), we know that there is
	 * something wrong with the RNG on this instance.
	 */
	if (!rand_success) {
		ts = GetCurrentTimestamp();
		memcpy(&gen_uuid->data[9], &ts, sizeof(TimestampTz));
	}

	gen_uuid->data[6] = (gen_uuid->data[6] & 0x0f) | 0x40;	/* "version" field */
	gen_uuid->data[8] = (gen_uuid->data[8] & 0x3f) | 0x80;	/* "variant" field */

	return gen_uuid;
}

TS_FUNCTION_INFO_V1(ts_uuid_generate);

Datum
ts_uuid_generate(PG_FUNCTION_ARGS)
{
	return UUIDPGetDatum(uuid_create());
}
