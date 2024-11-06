package observability

import (
	"context"
	"log"

	"go.opentelemetry.io/otel/trace"
)

// for testing
type simpleLog struct {
	data []any
}

func NewSimpleLog() *simpleLog {
	return &simpleLog{data: []any{}}
}

func (l simpleLog) Debug(msg string, attrs ...any) {
	attrs = append(attrs, l.data...)
	log.Println("debug: ", attrs)
}

func (l simpleLog) Error(msg string, err error, attrs ...any) {
	attrs = append(attrs, l.data...)
	log.Println("error: ", attrs)
}

func (l simpleLog) Info(msg string, attrs ...any) {
	attrs = append(attrs, l.data...)
	log.Println("info: ", attrs)
}

func (l simpleLog) Warn(msg string, attrs ...any) {
	attrs = append(attrs, l.data...)
	log.Println("warn: ", attrs)
}

func (l simpleLog) Fatal(msg string, attrs ...any) {
	attrs = append(attrs, l.data...)
	log.Fatal("fatal: ", attrs)
}

func (l *simpleLog) With(attrs ...any) Logger {
	l.data = append(l.data, attrs...)
	return l
}

func (l simpleLog) logWithSpan(ctx context.Context, attrs ...any) []any {
	attrs = append(attrs, l.data...)
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if spanCtx.IsValid() {
		attrs = append(attrs, "trace_id", spanCtx.TraceID().String())
		attrs = append(attrs, "span_id", spanCtx.SpanID().String())
	}
	return attrs
}

func (l simpleLog) InfoContext(ctx context.Context, msg string, attrs ...any) {
	log.Println("info ctx: ", msg, l.logWithSpan(ctx, attrs...))
}

func (l simpleLog) ErrorContext(ctx context.Context, msg string, err error, attrs ...any) {
	log.Println("error ctx: ", msg, l.logWithSpan(ctx, attrs...))
}

func (l simpleLog) DebugContext(ctx context.Context, msg string, attrs ...any) {
	log.Println("debug ctx: ", msg, l.logWithSpan(ctx, attrs...))
}

func (l simpleLog) WarnContext(ctx context.Context, msg string, attrs ...any) {
	log.Println("warn ctx: ", msg, l.logWithSpan(ctx, attrs...))
}

func (l simpleLog) FatalContext(ctx context.Context, msg string, attrs ...any) {
	log.Fatal("fatal ctx: ", msg, l.logWithSpan(ctx, attrs...))
}
