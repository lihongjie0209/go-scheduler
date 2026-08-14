package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
}

func TestLoadDiscoveryMode(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		coreTarget string
		wantErr    bool
	}{
		{name: "etcd remains the distributed default"},
		{name: "kubernetes uses configured DNS target", mode: "kubernetes", coreTarget: "dns:///core.platform.svc:9090"},
		{name: "kubernetes requires target", mode: "kubernetes", coreTarget: " ", wantErr: true},
		{name: "rejects unknown provider", mode: "consul", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			t.Setenv("DISCOVERY_MODE", test.mode)
			if test.mode == "kubernetes" {
				t.Setenv("CORE_GRPC_TARGET", test.coreTarget)
			}
			got, err := Load("api-server")
			if (err != nil) != test.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && test.mode == "kubernetes" && got.CoreGRPCTarget != test.coreTarget {
				t.Fatalf("CoreGRPCTarget = %q, want %q", got.CoreGRPCTarget, test.coreTarget)
			}
		})
	}
}

func TestKubernetesDiscoveryDoesNotValidateEtcdCredentials(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv("DISCOVERY_MODE", "kubernetes")
	t.Setenv("ETCD_CERT", "/unused/client.crt")
	t.Setenv("ETCD_KEY", "")
	if _, err := Load("scheduler-core"); err != nil {
		t.Fatalf("kubernetes discovery unexpectedly depends on etcd: %v", err)
	}
}

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

func TestLoadSMTPTransportMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	config, err := Load("scheduler-server")
	if err != nil {
		t.Fatal(err)
	}
	if config.SMTPTLSMode != "starttls" {
		t.Fatalf("default SMTP TLS mode = %q", config.SMTPTLSMode)
	}
	t.Setenv("SMTP_TLS_MODE", "TLS")
	config, err = Load("scheduler-server")
	if err != nil || config.SMTPTLSMode != "tls" {
		t.Fatalf("SMTP TLS mode = %q, %v", config.SMTPTLSMode, err)
	}
	t.Setenv("SMTP_TLS_MODE", "opportunistic")
	if _, err = Load("scheduler-server"); err == nil {
		t.Fatal("invalid SMTP TLS mode was accepted")
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

func TestLoadRejectsMalformedTypedEnvironment(t *testing.T) {
	tests := []struct {
		name, variable, value string
	}{
		{name: "workers", variable: "WORKERS", value: "many"},
		{name: "scheduler interval", variable: "SCHEDULER_INTERVAL", value: "often"},
		{name: "cookie secure", variable: "COOKIE_SECURE", value: "sometimes"},
		{name: "pool size", variable: "CORE_DATABASE_MAX_CONNS", value: "large"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://test")
			t.Setenv("SERVICE_TOKEN", "service-token")
			t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
			t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
			t.Setenv(test.variable, test.value)
			if _, err := Load("scheduler-server"); err == nil {
				t.Fatalf("Load() silently accepted %s=%s", test.variable, test.value)
			}
		})
	}
}
