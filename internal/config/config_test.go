package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

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
