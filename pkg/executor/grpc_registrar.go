package executor

import (
	"context"
	"errors"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

type GRPCRegistrarOptions struct {
	TenantID, GroupID, NodeID, Address string
	TTL                                time.Duration
	Labels                             []string
}

type GRPCRegistrar struct {
	client  schedulerv1.SchedulerServiceClient
	options GRPCRegistrarOptions
}

func NewGRPCRegistrar(client schedulerv1.SchedulerServiceClient, options GRPCRegistrarOptions) (*GRPCRegistrar, error) {
	if client == nil || options.TenantID == "" || options.GroupID == "" || options.NodeID == "" || options.Address == "" {
		return nil, errors.New("scheduler client, tenant, group, node, and address are required")
	}
	if options.TTL < 5*time.Second || options.TTL > 300*time.Second {
		return nil, errors.New("TTL must be between 5 and 300 seconds")
	}
	return &GRPCRegistrar{client: client, options: options}, nil
}

func (r *GRPCRegistrar) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.options.TTL / 3)
	defer func() {
		ticker.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = r.client.UnregisterExecutorNode(shutdownCtx, &schedulerv1.UnregisterExecutorNodeRequest{TenantId: r.options.TenantID, GroupId: r.options.GroupID, NodeId: r.options.NodeID})
	}()
	for {
		heartbeatCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, _ = r.client.RegisterExecutorNode(heartbeatCtx, &schedulerv1.RegisterExecutorNodeRequest{TenantId: r.options.TenantID, GroupId: r.options.GroupID, NodeId: r.options.NodeID, Address: r.options.Address, TtlSeconds: int32(r.options.TTL / time.Second), Labels: r.options.Labels})
		cancel()
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
