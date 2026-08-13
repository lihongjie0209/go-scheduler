package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ExecutorMetadata struct {
	TenantID  string    `json:"tenant_id"`
	GroupID   string    `json:"group_id"`
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ExecutorRegistry makes etcd the source of truth for dynamic executor
// liveness. PostgreSQL receives a TTL-bounded projection so existing
// transactional routing and audit logic remains deterministic.
type ExecutorRegistry struct {
	client *clientv3.Client
	prefix string
	store  ExecutorNodeStore
}

type ExecutorNodeStore interface {
	RegisterExecutorNode(context.Context, string, string, string, string, time.Duration) (store.ExecutorNode, error)
	UnregisterExecutorNode(context.Context, string, string, string) error
	ListExecutorNodes(context.Context, string, string, bool) ([]store.ExecutorNode, error)
}

func NewExecutorRegistry(client *clientv3.Client, prefix string, database ExecutorNodeStore) *ExecutorRegistry {
	return &ExecutorRegistry{client: client, prefix: path.Join(prefix, "executors"), store: database}
}

func (r *ExecutorRegistry) RegisterExecutorNode(ctx context.Context, tenantID, groupID, nodeID, address string, ttl time.Duration) (store.ExecutorNode, error) {
	metadata := ExecutorMetadata{TenantID: tenantID, GroupID: groupID, NodeID: nodeID, Address: address, UpdatedAt: time.Now().UTC()}
	value, err := json.Marshal(metadata)
	if err != nil {
		return store.ExecutorNode{}, err
	}
	lease, err := r.client.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		return store.ExecutorNode{}, fmt.Errorf("grant executor lease: %w", err)
	}
	key := r.key(tenantID, groupID, nodeID)
	if _, err = r.client.Put(ctx, key, string(value), clientv3.WithLease(lease.ID)); err != nil {
		_, _ = r.client.Revoke(context.WithoutCancel(ctx), lease.ID)
		return store.ExecutorNode{}, fmt.Errorf("publish executor: %w", err)
	}
	published, err := r.client.Get(ctx, key)
	if err != nil || len(published.Kvs) != 1 {
		_, _ = r.client.Revoke(context.WithoutCancel(ctx), lease.ID)
		if err != nil {
			return store.ExecutorNode{}, fmt.Errorf("read published executor: %w", err)
		}
		return store.ExecutorNode{}, fmt.Errorf("published executor disappeared")
	}
	revision := published.Kvs[0].ModRevision
	node, err := r.store.RegisterExecutorNode(ctx, tenantID, groupID, nodeID, address, ttl)
	if err != nil {
		rollbackCtx := context.WithoutCancel(ctx)
		_, _ = r.client.Txn(rollbackCtx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
			Then(clientv3.OpDelete(key)).Commit()
		_, _ = r.client.Revoke(rollbackCtx, lease.ID)
		return store.ExecutorNode{}, err
	}
	return node, nil
}

func (r *ExecutorRegistry) UnregisterExecutorNode(ctx context.Context, tenantID, groupID, nodeID string) error {
	if _, err := r.client.Delete(ctx, r.key(tenantID, groupID, nodeID)); err != nil {
		return fmt.Errorf("remove executor registration: %w", err)
	}
	return r.store.UnregisterExecutorNode(ctx, tenantID, groupID, nodeID)
}

func (r *ExecutorRegistry) ListExecutorNodes(ctx context.Context, tenantID, groupID string, liveOnly bool) ([]store.ExecutorNode, error) {
	stored, err := r.store.ListExecutorNodes(ctx, tenantID, groupID, false)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]store.ExecutorNode, len(stored))
	for _, node := range stored {
		if node.Static || !liveOnly {
			nodes[node.NodeID] = node
		}
	}
	response, err := r.client.Get(ctx, path.Join(r.prefix, tenantID, groupID)+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list executor registrations: %w", err)
	}
	for _, item := range response.Kvs {
		var metadata ExecutorMetadata
		if err = json.Unmarshal(item.Value, &metadata); err != nil || metadata.TenantID != tenantID || metadata.GroupID != groupID || metadata.NodeID == "" || metadata.Address == "" {
			continue
		}
		ttl, ttlErr := r.client.TimeToLive(ctx, clientv3.LeaseID(item.Lease))
		if ttlErr != nil || ttl.TTL <= 0 {
			continue
		}
		nodes[metadata.NodeID] = store.ExecutorNode{
			GroupID: metadata.GroupID, NodeID: metadata.NodeID, Address: metadata.Address,
			UpdatedAt: metadata.UpdatedAt, ExpiresAt: time.Now().Add(time.Duration(ttl.TTL) * time.Second),
		}
	}
	result := make([]store.ExecutorNode, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result, nil
}

func (r *ExecutorRegistry) key(tenantID, groupID, nodeID string) string {
	return path.Join(r.prefix, tenantID, groupID, nodeID)
}
