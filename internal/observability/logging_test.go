package observability

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogger(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewLogger(&output, LoggingConfig{Level: "warn", Format: "json", Service: "core", Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Warn("visible", "attempt", 2)

	line := output.String()
	for _, want := range []string{`"level":"WARN"`, `"msg":"visible"`, `"service":"core"`, `"version":"test"`, `"attempt":2`} {
		if !strings.Contains(line, want) {
			t.Errorf("log output %q does not contain %q", line, want)
		}
	}
	if strings.Contains(line, "hidden") {
		t.Errorf("info message was not filtered: %q", line)
	}
}

func TestNewLoggerRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		config LoggingConfig
	}{
		{name: "level", config: LoggingConfig{Level: "verbose", Format: "json"}},
		{name: "format", config: LoggingConfig{Level: slog.LevelInfo.String(), Format: "xml"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewLogger(&bytes.Buffer{}, test.config); err == nil {
				t.Errorf("NewLogger(%+v) succeeded", test.config)
			}
		})
	}
}

func TestNewLoggerSupportsTextAndNormalizedConfiguration(t *testing.T) {
	var output bytes.Buffer
	logger, err := NewLogger(&output, LoggingConfig{Level: " DEBUG ", Format: " TEXT ", AddSource: true, Service: "api-server", Version: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("configured")

	line := output.String()
	for _, want := range []string{"level=DEBUG", "msg=configured", "service=api-server", "version=v1", "source="} {
		if !strings.Contains(line, want) {
			t.Errorf("log output %q does not contain %q", line, want)
		}
	}
}
