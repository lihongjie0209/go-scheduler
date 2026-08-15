package store

import (
	"strings"
	"testing"
)

func TestUnregisteredOverrideAddresses(t *testing.T) {
	t.Parallel()
	nodes := []ExecutorNode{
		{Address: "http://worker-a:9999/"},
		{Address: " grpc://worker-c:9999 "},
	}
	if missing := unregisteredOverrideAddresses(nodes, []string{"http://worker-a:9999", "grpc://worker-c:9999/"}); len(missing) != 0 {
		t.Fatalf("registered addresses rejected: %v", missing)
	}
	missing := unregisteredOverrideAddresses(nodes, []string{"http://worker-a:9999", "http://evil:9999"})
	if strings.Join(missing, ",") != "http://evil:9999" {
		t.Fatalf("missing = %v", missing)
	}
}
