-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION get_telemetry_report(always_display_report boolean DEFAULT false) RETURNS text
    AS '@MODULE_PATHNAME@', 'ts_get_telemetry_report' LANGUAGE C STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_telemetry_report_json() RETURNS jsonb
    AS '@MODULE_PATHNAME@', 'ts_telemetry_get_report_jsonb' LANGUAGE C STABLE PARALLEL SAFE;
