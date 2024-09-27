package observability

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func NewZerolog(logServer string, id int) zerolog.Logger {
	stdOutput := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}
	lokiURL := logServer
	lokiClient := NewLokiClient(lokiURL)
	lokiHook := NewLokiHook(lokiClient, id)

	output := io.MultiWriter(stdOutput, lokiHook)

	logger := zerolog.New(output).With().Timestamp().Logger()
	// logger.Hook(observability.TracingHook{})

	logger = logger.Output(lokiHook)

	return logger
}
