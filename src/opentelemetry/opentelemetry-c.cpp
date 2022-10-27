/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/provider.h>

#include "opentelemetry-c.h"

namespace trace = opentelemetry::trace;
namespace nostd = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace otlp = opentelemetry::exporter::otlp;

struct TracerProvider
{
	nostd::shared_ptr<trace::TracerProvider> provider;

	TracerProvider(nostd::shared_ptr<trace::TracerProvider> provider) : provider(provider){};
};

struct Tracer
{
	nostd::shared_ptr<trace::Tracer> tracer;

	Tracer(nostd::shared_ptr<trace::Tracer> tracer) : tracer(tracer){};
};

struct Span
{
	nostd::shared_ptr<trace::Span> span;
	Span(nostd::shared_ptr<trace::Span> span) : span(span){};
};

struct Scope
{
	trace::Scope scope;
	Scope(nostd::shared_ptr<trace::Tracer> &tracer, nostd::shared_ptr<trace::Span> &span)
											: scope(tracer->WithActiveSpan(span)){

											  };
};

opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;

void
ts_opentelemetry_tracer_init(void)
{
	auto exporter = otlp::OtlpHttpExporterFactory::Create(opts);
	auto processor = trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));
	std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
		trace_sdk::TracerProviderFactory::Create(std::move(processor));
	// Set the global trace provider
	trace::Provider::SetTracerProvider(provider);
}

struct TracerProvider *
ts_opentelemetry_tracer_provider_get(void)
{
	// struct TracerProvider *p = static_cast<struct TracerProvider *>(malloc(sizeof(struct
	// TracerProvider)));
	struct TracerProvider *p =
		new (std::nothrow) struct TracerProvider(trace::Provider::GetTracerProvider());
	// p->provider = trace::Provider::GetTracerProvider();
	return p;
}

void
ts_opentelemetry_tracer_provider_delete(struct TracerProvider *tp)
{
	// delete &tp->provider;
	delete tp; // free(tp);
}

struct Tracer *
ts_opentelemetry_tracer_get(struct TracerProvider *tp, const char *library_name,
							const char *version)
{
	// struct Tracer *t = static_cast<struct Tracer *>(malloc(sizeof(struct Tracer)));
	struct Tracer *t =
		new (std::nothrow) struct Tracer(tp->provider->GetTracer(library_name, version));
	// t->tracer = tp->provider->GetTracer(library_name, version);
	return t;
}

void
ts_opentelemetry_tracer_delete(struct Tracer *t)
{
	// delete &t->tracer;
	delete t;
	// free(t);
}

struct Span *
ts_opentelemetry_span_start(struct Tracer *t, const char *name)
{
	// struct Span *s = static_cast<struct Span *>(malloc(sizeof(struct Span)));
	// s->span = t->tracer->StartSpan(name);
	struct Span *s = new (std::nothrow) struct Span(t->tracer->StartSpan(name));
	return s;
}

void
ts_opentelemetry_span_add_event(struct Span *s, const char *name)
{
	s->span->AddEvent(name);
}

void
ts_opentelemetry_span_set_attribute(struct Span *s, const char *key, const char *value)
{
	s->span->SetAttribute(key, value);
}

void
ts_opentelemetry_span_end(struct Span *s)
{
	s->span->End();
}

void
ts_opentelemetry_span_delete(struct Span *s)
{
	// delete &s->span;
	// free(s);
	delete s;
}

struct Scope *
ts_opentelemetry_tracer_with_active_span(struct Tracer *t, struct Span *s)
{
	// struct Scope *sc = static_cast<struct Scope *>(malloc(sizeof(struct Scope)));
	// sc->scope = t->tracer->WithActiveSpan(s->span);
	struct Scope *sc = new (std::nothrow) struct Scope(t->tracer, s->span);
	return sc;
}

void
ts_opentelemetry_scope_delete(struct Scope *sc)
{
	// delete &sc->scope;
	// free(sc);
	delete sc;
}
