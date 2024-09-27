package observability

import (
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

type TracingHook struct{}

func (h TracingHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	ctx := e.GetCtx()
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		traceID := span.SpanContext().TraceID()
		spanID := span.SpanContext().SpanID()
		e.Str("trace_id", traceID.String())
		e.Str("span_id", spanID.String())
	}
}
