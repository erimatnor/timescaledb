/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "telemetry.h"
#include <utils/builtins.h>
#include <jsonb_utils.h>
#include "hypertable.h"
#include "telemetry/telemetry.h"
#include "dist_util.h"
#include "data_node.h"

#define DISTRIBUTED_DB_KEY "distributed_db"
#define NUM_DATA_NODES_KEY "num_data_nodes"

static void
tsl_telemetry_add_distributed_database_info(JsonbParseState *parse_state)
{
	DistUtilMembershipStatus status = dist_util_membership();

	if (status == DIST_MEMBER_NONE)
		return;

	ts_jsonb_add_int64(parse_state,
					   NUM_DATA_NODES_KEY,
					   list_length(data_node_get_node_name_list()));
}

void
tsl_telemetry_add_info(JsonbParseState **parse_state)
{
	JsonbValue distributed_db_key;

	/* distributed_db */
	distributed_db_key.type = jbvString;
	distributed_db_key.val.string.val = DISTRIBUTED_DB_KEY;
	distributed_db_key.val.string.len = strlen(DISTRIBUTED_DB_KEY);
	pushJsonbValue(parse_state, WJB_KEY, &distributed_db_key);
	pushJsonbValue(parse_state, WJB_BEGIN_OBJECT, NULL);
	tsl_telemetry_add_distributed_database_info(*parse_state);
	pushJsonbValue(parse_state, WJB_END_OBJECT, NULL);
}
