/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/provider.h>
#include "opentelemetry-c.h"

struct TracerProvider {
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> provider;
};

struct Tracer {
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;
};

struct Span {
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
};

struct Scope {
    opentelemetry::trace::Scope scope;
};

struct TracerProvider *
ts_opentelemetry_provider_get(void)
{
    struct TracerProvider *p = static_cast<struct TracerProvider *>(malloc(sizeof(struct TracerProvider)));    
    p->provider = opentelemetry::trace::Provider::GetTracerProvider();
    return p;
}

void
ts_opentelemetry_tracer_provider_delete(struct TracerProvider *tp)
{
    delete &tp->provider;
    free(tp);
}
    
struct Tracer *
ts_opentelemetry_tracer_get(struct TracerProvider *tp, const char *library_name, const char *version)
{
    struct Tracer *t = static_cast<struct Tracer *>(malloc(sizeof(struct Tracer)));
    t->tracer = tp->provider->GetTracer(library_name, version);
    return t;
}

void
ts_opentelemetry_tracer_delete(struct Tracer *t)
{
    delete &t->tracer;
    free(t);
}

struct Span *
ts_opentelemetry_span_start(struct Tracer *t, const char *name)
{
    struct Span *s = static_cast<struct Span *>(malloc(sizeof(struct Span)));
    s->span = t->tracer->StartSpan(name);
    return s;
}

void
ts_opentelemetry_span_end(struct Span *s)
{
    s->span->End();
}


void
ts_opentelemetry_span_delete(struct Span *s)
{
    delete &s->span;
    free(s);
}

struct Scope *
ts_opentelemetry_tracer_with_active_span(struct Tracer *t, struct Span *s)
{
    struct Scope *sc = static_cast<struct Scope *>(malloc(sizeof(struct Scope)));
    sc->scope = t->tracer->WithActiveSpan(s->span);
    return sc;
}

void
ts_opentelemetry_scope_delete(struct Scope *sc)
{
    delete &sc->scope;
    free(sc);
}

