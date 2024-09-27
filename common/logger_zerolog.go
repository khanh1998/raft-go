package common

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

type zerologLogger struct {
	logger zerolog.Logger
}

// NewLogger initializes the logger with Zerolog.
func NewLogger() Logger {
	zero := zerolog.New(os.Stdout).With().Timestamp().Logger()
	return &zerologLogger{logger: zero}
}

// With creates a new logger with additional attributes.
func (l *zerologLogger) With(attrs ...any) Logger {
	newLogger := l.logger.With().Fields(convertAttrsToMap(attrs...)).Logger()
	return &zerologLogger{logger: newLogger}
}

// Info logs an informational message with attributes.
func (l *zerologLogger) Info(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.InfoLevel, msg, attrs...)
}

// Error logs an error message with attributes.
func (l *zerologLogger) Error(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.ErrorLevel, msg, attrs...)
}

// Debug logs a debug message with attributes.
func (l *zerologLogger) Debug(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.DebugLevel, msg, attrs...)
}

// Warn logs a warning message with attributes.
func (l *zerologLogger) Warn(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.WarnLevel, msg, attrs...)
}

// logWithSpan adds OpenTelemetry span data to logs if available.
func (l *zerologLogger) logWithSpan(ctx context.Context, level zerolog.Level, msg string, attrs ...any) {
	span := trace.SpanFromContext(ctx)
	event := l.logger.WithLevel(level).
		Str("trace_id", span.SpanContext().TraceID().String()).
		Str("span_id", span.SpanContext().SpanID().String()).
		Fields(convertAttrsToMap(attrs...))

	event.Msg(msg)
}

// convertAttrsToMap converts variadic attributes to a map for structured logging.
func convertAttrsToMap(attrs ...any) map[string]interface{} {
	fields := make(map[string]interface{})
	for i := 0; i < len(attrs); i += 2 {
		if i+1 < len(attrs) {
			key, ok := attrs[i].(string)
			if ok {
				fields[key] = attrs[i+1]
			}
		}
	}
	return fields
}
