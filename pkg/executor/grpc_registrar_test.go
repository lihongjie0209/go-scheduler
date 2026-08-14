package executor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc"
)

type failingRegistrarClient struct {
	schedulerv1.SchedulerServiceClient
	cancel       context.CancelFunc
	unregistered bool
}

func (c *failingRegistrarClient) RegisterExecutorNode(context.Context, *schedulerv1.RegisterExecutorNodeRequest, ...grpc.CallOption) (*schedulerv1.ExecutorNode, error) {
	c.cancel()
	return nil, errors.New("tenant not found")
}

func (c *failingRegistrarClient) UnregisterExecutorNode(context.Context, *schedulerv1.UnregisterExecutorNodeRequest, ...grpc.CallOption) (*schedulerv1.UnregisterExecutorNodeResponse, error) {
	c.unregistered = true
	return &schedulerv1.UnregisterExecutorNodeResponse{}, nil
}

func TestGRPCRegistrar_LogsFailedHeartbeatAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := &failingRegistrarClient{cancel: cancel}
	var output bytes.Buffer
	registrar := &GRPCRegistrar{
		client: client,
		options: GRPCRegistrarOptions{
			TenantID: "tenant-1", GroupID: "group-1", NodeID: "node-1",
			Address: "grpc://10.0.0.2:9999", TTL: 30 * time.Second,
		},
		logger: slog.New(slog.NewJSONHandler(&output, nil)),
	}

	if err := registrar.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !client.unregistered {
		t.Fatal("executor was not unregistered during shutdown")
	}
	logOutput := output.String()
	for _, expected := range []string{"executor heartbeat registration failed", "tenant-1", "group-1", "node-1", "tenant not found"} {
		if !strings.Contains(logOutput, expected) {
			t.Fatalf("heartbeat log %q does not contain %q", logOutput, expected)
		}
	}
}
