/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <parser/analyze.h>

#include "config.h"
#include "compat/compat.h"
#include "extension_constants.h"
#include "opentelemetry/opentelemetry-c.h"
#include <utils/memutils.h>

static struct TracerProvider *tracer_provider;
static struct Tracer *tracer;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

void _tracing_init(void);
void _tracing_fini(void);

static struct TxTraceContext
{
	HTAB *query_spans;
	struct Span *tx_span;
	struct Scope *tx_scope;
} *tx_trace_context = NULL;

static void
ts_post_parse_analyze_hook(ParseState *pstate, Query *query
#if PG14_GE
						   ,
						   JumbleState *jstate
#endif
)
{
	if (NULL == tx_trace_context && GetCurrentTransactionNestLevel() == 1)
	{
		MemoryContext oldmcxt = MemoryContextSwitchTo(TopTransactionContext);
		struct Span *span = ts_opentelemetry_span_start(tracer, "Transaction");
		struct Scope *scope = ts_opentelemetry_tracer_with_active_span(tracer, span);
		ts_opentelemetry_span_set_attribute(span,
											"TransactionId",
											psprintf("%u", GetCurrentTransactionId()));
		ts_opentelemetry_span_set_attribute(span,
											"CommandId",
											psprintf("%u", GetCurrentCommandId(false)));
		tx_trace_context = palloc(sizeof(struct TxTraceContext));
		tx_trace_context->tx_span = span;
		tx_trace_context->tx_scope = scope;
		MemoryContextSwitchTo(oldmcxt);
	}

	// struct Span *query_span = ts_opentelemetry_span_start(tracer, psprintf("Query %u",
	// GetCurrentTransactionId())); struct Scope *query_scope =
	// ts_opentelemetry_tracer_with_active_span(tracer, query_span);

	if (prev_post_parse_analyze_hook != NULL)
	{
#if PG14_LT
		prev_post_parse_analyze_hook(pstate, query);
#else
		prev_post_parse_analyze_hook(pstate, query, jstate);
#endif
	}
}

static void
tracing_xact_callback(XactEvent event, void *arg)
{
	if (NULL == tx_trace_context)
		return;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
			ts_opentelemetry_span_add_event(tx_trace_context->tx_span,
											(event == XACT_EVENT_COMMIT) ? "commit" : "abort");
			ts_opentelemetry_span_end(tx_trace_context->tx_span);
			ts_opentelemetry_scope_delete(tx_trace_context->tx_scope);
			ts_opentelemetry_span_delete(tx_trace_context->tx_span);
			tx_trace_context = NULL;
			break;
		default:
			break;
	}
}

static void
tracing_subxact_callback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid,
						 void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:
			break;
		case SUBXACT_EVENT_ABORT_SUB:
		case SUBXACT_EVENT_COMMIT_SUB:
			break;
		default:
			break;
	}
}

void
_tracing_init(void)
{
	ts_opentelemetry_tracer_init();
	tracer_provider = ts_opentelemetry_tracer_provider_get();
	tracer = ts_opentelemetry_tracer_get(tracer_provider, EXTENSION_NAME, TIMESCALEDB_VERSION_MOD);
	RegisterXactCallback(tracing_xact_callback, NULL);
	RegisterSubXactCallback(tracing_subxact_callback, NULL);
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = ts_post_parse_analyze_hook;
}

void
_tracing_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	prev_post_parse_analyze_hook = NULL;
	UnregisterXactCallback(tracing_xact_callback, NULL);
	UnregisterSubXactCallback(tracing_subxact_callback, NULL);
	ts_opentelemetry_tracer_provider_delete(tracer_provider);
	ts_opentelemetry_tracer_delete(tracer);
}
