package discovery

import "testing"

func TestExecutorRegistryKeyIsScopedByTenantAndGroup(t *testing.T) {
	registry := &ExecutorRegistry{prefix: "/go-scheduler/prod/executors"}
	got := registry.key("tenant-1", "group-1", "node-1")
	want := "/go-scheduler/prod/executors/tenant-1/group-1/node-1"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}
