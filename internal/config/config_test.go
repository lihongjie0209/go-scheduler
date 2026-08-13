package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestNormalizeContextPath(t *testing.T) {
	tests := []struct {
		name, input, want string
		wantErr           bool
	}{
		{name: "empty", input: "", want: ""},
		{name: "root", input: "/", want: ""},
		{name: "adds leading slash", input: "scheduler", want: "/scheduler"},
		{name: "removes trailing slash", input: "/scheduler/", want: "/scheduler"},
		{name: "nested", input: "/platform/scheduler", want: "/platform/scheduler"},
		{name: "rejects traversal", input: "/platform/../scheduler", wantErr: true},
		{name: "rejects duplicate slash", input: "/platform//scheduler", wantErr: true},
		{name: "rejects query", input: "/scheduler?debug=true", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeContextPath(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("normalizeContextPath(%q) error = %v, wantErr %v", test.input, err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("normalizeContextPath(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestAppendContextPath(t *testing.T) {
	if got := appendContextPath("https://scheduler.example.com/", "/scheduler"); got != "https://scheduler.example.com/scheduler" {
		t.Fatalf("appendContextPath() = %q", got)
	}
	if got := appendContextPath("https://scheduler.example.com/scheduler", "/scheduler"); got != "https://scheduler.example.com/scheduler" {
		t.Fatalf("appendContextPath() duplicated path: %q", got)
	}
}

func TestStandaloneLoadDoesNotRequireEtcdConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("ETCD_CERT", "/unused/client.crt")
	t.Setenv("ETCD_KEY", "")
	if _, err := Load("scheduler-server"); err != nil {
		t.Fatalf("standalone config unexpectedly depends on etcd: %v", err)
	}
}

func TestLoadRejectsNonPositiveSchedulerConcurrency(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
	}{
		{name: "zero workers", variable: "WORKERS", value: "0"},
		{name: "negative workers", variable: "WORKERS", value: "-1"},
		{name: "zero interval", variable: "SCHEDULER_INTERVAL", value: "0s"},
		{name: "negative interval", variable: "SCHEDULER_INTERVAL", value: "-1s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test")
			t.Setenv("SERVICE_TOKEN", "service-token")
			t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
			t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
			t.Setenv(test.variable, test.value)
			if _, err := Load("scheduler-server"); err == nil {
				t.Fatalf("Load() accepted %s=%s", test.variable, test.value)
			}
		})
	}
}

func TestLoadDatabasePoolConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("API_DATABASE_MAX_CONNS", "6")
	t.Setenv("API_DATABASE_MIN_CONNS", "2")
	t.Setenv("CORE_DATABASE_MAX_CONNS", "18")
	t.Setenv("CORE_DATABASE_MIN_CONNS", "3")

	config, err := Load("scheduler-server")
	if err != nil {
		t.Fatal(err)
	}
	if config.APIDatabaseMaxConns != 6 || config.APIDatabaseMinConns != 2 || config.CoreDatabaseMaxConns != 18 || config.CoreDatabaseMinConns != 3 {
		t.Fatalf("database pool configuration = %+v", config)
	}
}

func TestLoadRejectsInvalidDatabasePoolConfiguration(t *testing.T) {
	tests := []struct {
		name, variable, value string
	}{
		{name: "zero API max", variable: "API_DATABASE_MAX_CONNS", value: "0"},
		{name: "negative API min", variable: "API_DATABASE_MIN_CONNS", value: "-1"},
		{name: "API min exceeds max", variable: "API_DATABASE_MIN_CONNS", value: "9"},
		{name: "zero Core max", variable: "CORE_DATABASE_MAX_CONNS", value: "0"},
		{name: "Core min exceeds max", variable: "CORE_DATABASE_MIN_CONNS", value: "25"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test")
			t.Setenv("SERVICE_TOKEN", "service-token")
			t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
			t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
			t.Setenv(test.variable, test.value)
			if _, err := Load("scheduler-server"); err == nil {
				t.Fatalf("Load() accepted %s=%s", test.variable, test.value)
			}
		})
	}
}
