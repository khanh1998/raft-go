package observability

import "context"

type Logger interface {
	Info(msg string, attrs ...any)
	Error(msg string, err error, attrs ...any)
	Debug(msg string, attrs ...any)
	Warn(msg string, attrs ...any)
	Fatal(msg string, attrs ...any)

	InfoContext(ctx context.Context, msg string, attrs ...any)
	ErrorContext(ctx context.Context, msg string, err error, attrs ...any)
	DebugContext(ctx context.Context, msg string, attrs ...any)
	WarnContext(ctx context.Context, msg string, attrs ...any)
	FatalContext(ctx context.Context, msg string, attrs ...any)
	With(attrs ...any) Logger
}
