package observability

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

// otelLogger implements the Logger interface using otelslog.
type otelLogger struct {
	logger *slog.Logger
	attrs  []any
}

// Fatal implements Logger.
func (l *otelLogger) Fatal(msg string, attrs ...any) {
	l.logger.Error(msg, attrs...)
	os.Exit(1)
}

// Debug implements Logger.
func (l *otelLogger) Debug(msg string, attrs ...any) {
	l.logger.Debug(msg, attrs...)
}

// Error implements Logger.
func (l *otelLogger) Error(msg string, err error, attrs ...any) {
	if err != nil {
		attrs = append(attrs, err.Error())
	}
	l.logger.Error(msg, attrs...)
}

// Info implements Logger.
func (l *otelLogger) Info(msg string, attrs ...any) {
	l.logger.Info(msg, attrs...)
}

// Warn implements Logger.
func (l *otelLogger) Warn(msg string, attrs ...any) {
	l.logger.Warn(msg, attrs...)
}

func NewOtelLogger() Logger {
	otelSlogLogger := otelslog.NewLogger("otelslog")
	return &otelLogger{logger: otelSlogLogger}
}

func (l *otelLogger) InfoContext(ctx context.Context, msg string, fields ...any) {
	l.logger.InfoContext(ctx, msg, fields...)
}

func (l *otelLogger) ErrorContext(ctx context.Context, msg string, err error, fields ...any) {
	if err != nil {
		fields = append(fields, err.Error())
	}
	l.logger.ErrorContext(ctx, msg, fields...)
}

func (l *otelLogger) DebugContext(ctx context.Context, msg string, fields ...any) {
	l.logger.DebugContext(ctx, msg, fields...)
}

func (l *otelLogger) WarnContext(ctx context.Context, msg string, fields ...any) {
	l.logger.WarnContext(ctx, msg, fields...)
}

func (l *otelLogger) FatalContext(ctx context.Context, msg string, fields ...any) {
	l.logger.ErrorContext(ctx, msg, fields...)
	os.Exit(1)
}

func (l *otelLogger) With(fields ...any) Logger {
	return &otelLogger{
		logger: l.logger,
		attrs:  append(l.attrs, fields...),
	}
}
