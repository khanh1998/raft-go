package observability

import (
	"context"
	"io"
	"khanh/raft-go/common"
	"os"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

type zerologLogger struct {
	logger zerolog.Logger
}

func (l *zerologLogger) Debug(msg string, attrs ...any) {
	l.log(zerolog.DebugLevel, msg, attrs...)
}

func (l *zerologLogger) Error(msg string, err error, attrs ...any) {
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	l.log(zerolog.ErrorLevel, msg, attrs...)
}

func (l *zerologLogger) Info(msg string, attrs ...any) {
	l.log(zerolog.InfoLevel, msg, attrs...)
}

func (l *zerologLogger) Warn(msg string, attrs ...any) {
	l.log(zerolog.WarnLevel, msg, attrs...)
}

func (l *zerologLogger) Fatal(msg string, attrs ...any) {
	l.log(zerolog.FatalLevel, msg, attrs...)
	os.Exit(1)
}

func NewZerologForTest() Logger {
	stdOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}
	logger := zerolog.New(stdOutput).With().Timestamp().Logger()
	return &zerologLogger{logger}
}

func NewZerolog(cfg common.ObservabilityConfig, id int) Logger {
	stdOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}
	var logger zerolog.Logger

	if !cfg.Disabled && cfg.LokiPushURL != "" {
		lokiClient := NewLokiClient(cfg.LokiPushURL)
		lokiHook := NewLokiHook(lokiClient, id)
		output := io.MultiWriter(stdOutput, lokiHook)
		logger = zerolog.New(output).With().Timestamp().Logger()
	} else {
		logger = zerolog.New(stdOutput).With().Timestamp().Logger()
	}

	// logger.Hook(observability.TracingHook{})

	return &zerologLogger{logger}
}

func (l *zerologLogger) With(attrs ...any) Logger {
	newLogger := l.logger.With().Fields(convertAttrsToMap(attrs...)).Logger()
	return &zerologLogger{logger: newLogger}
}

func (l *zerologLogger) InfoContext(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.InfoLevel, msg, attrs...)
}

func (l *zerologLogger) ErrorContext(ctx context.Context, msg string, err error, attrs ...any) {
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	l.logWithSpan(ctx, zerolog.ErrorLevel, msg, attrs...)
}

func (l *zerologLogger) DebugContext(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.DebugLevel, msg, attrs...)
}

func (l *zerologLogger) WarnContext(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.WarnLevel, msg, attrs...)
}

func (l *zerologLogger) FatalContext(ctx context.Context, msg string, attrs ...any) {
	l.logWithSpan(ctx, zerolog.FatalLevel, msg, attrs...)
	os.Exit(1)
}

func (l *zerologLogger) logWithSpan(ctx context.Context, level zerolog.Level, msg string, attrs ...any) {
	var event *zerolog.Event
	spanCtx := trace.SpanFromContext(ctx).SpanContext()
	if spanCtx.IsValid() {
		event = l.logger.WithLevel(level).
			Str("trace_id", spanCtx.TraceID().String()).
			Str("span_id", spanCtx.SpanID().String()).
			Fields(convertAttrsToMap(attrs...))
	} else {
		event = l.logger.WithLevel(level).
			Fields(convertAttrsToMap(attrs...))
	}

	event.Msg(msg)
}

func (l *zerologLogger) log(level zerolog.Level, msg string, attrs ...any) {
	event := l.logger.WithLevel(level).Fields(convertAttrsToMap(attrs...))

	event.Msg(msg)
}

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
