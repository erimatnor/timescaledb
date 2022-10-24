/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/provider.h>
#include "opentelemetry-c.h"

TracerProvider *
ts_opentelemetry_get_provider(void)
{
	return trace::Provider::GetTracerProvider().get();
}
