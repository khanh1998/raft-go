package common

import "context"

type Logger interface {
	Info(ctx context.Context, msg string, attrs ...any)
	Error(ctx context.Context, msg string, attrs ...any)
	Debug(ctx context.Context, msg string, attrs ...any)
	Warn(ctx context.Context, msg string, attrs ...any)
	With(attrs ...any) Logger
}
