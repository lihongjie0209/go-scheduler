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

func TestLoadRequiresTargetAllowlist(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("TARGET_HOST_ALLOWLIST", "")
	if _, err := Load("test"); err == nil {
		t.Fatal("expected missing allowlist error")
	}
}
func TestLoadParsesTargetAllowlist(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("TARGET_HOST_ALLOWLIST", " API.EXAMPLE.COM, *.jobs.example.org ")
	config, err := Load("test")
	if err != nil {
		t.Fatal(err)
	}
	if len(config.TargetAllowlist) != 2 || config.TargetAllowlist[0] != "api.example.com" {
		t.Fatalf("unexpected allowlist: %#v", config.TargetAllowlist)
	}
}

func TestStandaloneLoadDoesNotRequireEtcdConfiguration(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test")
	t.Setenv("SERVICE_TOKEN", "service-token")
	t.Setenv("MASTER_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("JWT_SECRET", strings.Repeat("j", 32))
	t.Setenv("TARGET_HOST_ALLOWLIST", "example.com")
	t.Setenv("ETCD_CERT", "/unused/client.crt")
	t.Setenv("ETCD_KEY", "")
	if _, err := Load("scheduler-server"); err != nil {
		t.Fatalf("standalone config unexpectedly depends on etcd: %v", err)
	}
}
