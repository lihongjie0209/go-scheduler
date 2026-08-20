package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// LoggingConfig controls the process-wide structured logger.
type LoggingConfig struct {
	Level     string
	Format    string
	AddSource bool
	Service   string
	Version   string
}

// NewLogger creates a structured logger suitable for both application and
// framework lifecycle events.
func NewLogger(output io.Writer, config LoggingConfig) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(config.Level)))); err != nil {
		return nil, fmt.Errorf("invalid log level %q: use debug, info, warn, or error", config.Level)
	}

	options := &slog.HandlerOptions{Level: level, AddSource: config.AddSource}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(config.Format)) {
	case "json":
		handler = slog.NewJSONHandler(output, options)
	case "text":
		handler = slog.NewTextHandler(output, options)
	default:
		return nil, fmt.Errorf("invalid log format %q: use json or text", config.Format)
	}

	return slog.New(handler).With("service", config.Service, "version", config.Version), nil
}
