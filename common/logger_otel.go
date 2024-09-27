package common

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

// otelLogger implements the Logger interface using otelslog.
type otelLogger struct {
	logger *slog.Logger
	attrs  []any
}

func NewOtelLogger() Logger {
	otelSlogLogger := otelslog.NewLogger("")
	return &otelLogger{logger: otelSlogLogger}
}

func (l *otelLogger) Info(ctx context.Context, msg string, fields ...any) {
	l.logger.InfoContext(ctx, msg, fields...)
}

func (l *otelLogger) Error(ctx context.Context, msg string, fields ...any) {
	l.logger.InfoContext(ctx, msg, fields...)
}

func (l *otelLogger) Debug(ctx context.Context, msg string, fields ...any) {
	l.logger.InfoContext(ctx, msg, fields...)
}

func (l *otelLogger) Warn(ctx context.Context, msg string, fields ...any) {
	l.logger.InfoContext(ctx, msg, fields...)
}

func (l *otelLogger) With(fields ...any) Logger {
	return &otelLogger{
		logger: l.logger,
		attrs:  append(l.attrs, fields...),
	}
}
