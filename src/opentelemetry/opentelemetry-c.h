/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_OPENTELEMETRY_C_H
#define TIMESCALEDB_OPENTELEMETRY_C_H

#ifdef __cplusplus
extern "C" {
#endif
	struct TracerProvider;
	struct Tracer;
	struct Span;
	struct Scope;
	
	struct TracerProvider *ts_opentelemetry_provider_get();
	void ts_opentelemetry_tracer_provider_delete(struct TracerProvider *tp);
	
	struct Tracer *ts_opentelemetry_tracer_get(struct TracerProvider *tp, const char *library_name, const char *version);
    void ts_opentelemetry_tracer_delete(struct Tracer *t);

	struct Span *ts_opentelemetry_span_start(struct Tracer *t);
	void ts_opentelemetry_span_end(struct Span *s);
	void ts_opentelemetry_span_delete(struct Span *s);

	struct Scope *ts_opentelemetry_tracer_with_active_span(struct Tracer *t, struct Span *s);	
	void ts_opentelemetry_scope_delete(struct Scope *sc);
	
#ifdef __cplusplus
}
#endif

#endif /* TIMESCALEDB_OPENTELEMETRY_C_H */
