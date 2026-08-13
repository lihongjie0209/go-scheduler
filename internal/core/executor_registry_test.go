package core

import (
	"context"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type recordingExecutorRegistry struct {
	registered store.ExecutorNode
	ttl        time.Duration
}

func (r *recordingExecutorRegistry) RegisterExecutorNode(_ context.Context, _, groupID, nodeID, address string, ttl time.Duration, labels ...[]string) (store.ExecutorNode, error) {
	r.registered = store.ExecutorNode{GroupID: groupID, NodeID: nodeID, Address: address, ExpiresAt: time.Now().Add(ttl), UpdatedAt: time.Now()}
	r.ttl = ttl
	return r.registered, nil
}
func (*recordingExecutorRegistry) UnregisterExecutorNode(context.Context, string, string, string) error {
	return nil
}
func (r *recordingExecutorRegistry) ListExecutorNodes(context.Context, string, string, bool) ([]store.ExecutorNode, error) {
	return []store.ExecutorNode{r.registered}, nil
}

func TestServiceUsesConfiguredExecutorRegistry(t *testing.T) {
	registry := &recordingExecutorRegistry{}
	service := NewService(nil, registry)
	node, err := service.RegisterExecutorNode(t.Context(), &schedulerv1.RegisterExecutorNodeRequest{
		TenantId: "tenant-1", GroupId: "group-1", NodeId: " node-1 ",
		Address: "http://executor:9999/", TtlSeconds: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if registry.registered.NodeID != "node-1" || registry.registered.Address != "http://executor:9999" || registry.ttl != 30*time.Second {
		t.Fatalf("registration = %+v, ttl = %s", registry.registered, registry.ttl)
	}
	if node.GetNodeId() != "node-1" {
		t.Fatalf("node ID = %q", node.GetNodeId())
	}
}
