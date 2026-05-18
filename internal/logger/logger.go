package logger

import (
	"io"
	"log/slog"
	"strings"
)

// Init creates a new slog logger and sets it as the default one.
//
// `level` should be one of "debug", "info", "warn" and "error".
//
// If `pretty` is true, logs will follow key=value format, otherwise JSON format.
func Init(destination io.Writer, level string, pretty bool) {
	var slogLevel slog.Level

	// Convert the given log-level to slog.Level case-insensitively.
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		panic("unknown log level provided: " + level)
	}

	// Set logger level.
	options := &slog.HandlerOptions{AddSource: true, Level: slogLevel}

	var handler slog.Handler
	if pretty {
		handler = slog.NewTextHandler(destination, options)
	} else {
		handler = slog.NewJSONHandler(destination, options)
	}

	handler = ContextHandler{Handler: handler}
	slog.SetDefault(slog.New(handler))
}
