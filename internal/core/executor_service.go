package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

var errExecutorGroupRequired = errors.New("tenant_id and name are required")

type ExecutorGroupRepository interface {
	CreateExecutorGroup(context.Context, store.ExecutorGroup) (store.ExecutorGroup, error)
	UpdateExecutorGroup(context.Context, store.ExecutorGroup) (store.ExecutorGroup, error)
	DeleteExecutorGroup(context.Context, string, string, int64) error
	ListExecutorGroups(context.Context, string) ([]store.ExecutorGroup, error)
}

type ExecutorService struct {
	groups   ExecutorGroupRepository
	registry ExecutorRegistry
}

type RegisterExecutorNodeInput struct {
	TenantID   string
	GroupID    string
	NodeID     string
	Address    string
	TTLSeconds int32
	Labels     []string
}

func NewExecutorService(groups ExecutorGroupRepository, registry ExecutorRegistry) *ExecutorService {
	return &ExecutorService{groups: groups, registry: registry}
}

func (s *ExecutorService) CreateGroup(ctx context.Context, group store.ExecutorGroup) (store.ExecutorGroup, error) {
	normalized, err := normalizeExecutorGroupModel(group)
	if err != nil {
		return store.ExecutorGroup{}, &ValidationError{err: err}
	}
	return s.groups.CreateExecutorGroup(ctx, normalized)
}

func (s *ExecutorService) UpdateGroup(ctx context.Context, group store.ExecutorGroup) (store.ExecutorGroup, error) {
	normalized, err := normalizeExecutorGroupModel(group)
	if err != nil {
		return store.ExecutorGroup{}, &ValidationError{err: err}
	}
	if normalized.ID == "" || normalized.Version < 1 {
		return store.ExecutorGroup{}, &ValidationError{err: fmt.Errorf("id and positive version are required")}
	}
	return s.groups.UpdateExecutorGroup(ctx, normalized)
}

func (s *ExecutorService) DeleteGroup(ctx context.Context, tenantID, id string, version int64) error {
	if tenantID == "" || id == "" || version < 1 {
		return &ValidationError{err: fmt.Errorf("tenant_id, id and positive version are required")}
	}
	return s.groups.DeleteExecutorGroup(ctx, tenantID, id, version)
}

func (s *ExecutorService) ListGroups(ctx context.Context, tenantID string) ([]store.ExecutorGroup, error) {
	return s.groups.ListExecutorGroups(ctx, tenantID)
}

func (s *ExecutorService) RegisterNode(ctx context.Context, input RegisterExecutorNodeInput) (store.ExecutorNode, error) {
	if input.TenantID == "" || input.GroupID == "" || strings.TrimSpace(input.NodeID) == "" {
		return store.ExecutorNode{}, &ValidationError{err: fmt.Errorf("tenant_id, group_id and node_id are required")}
	}
	parsed, err := url.ParseRequestURI(input.Address)
	if err != nil || parsed.Host == "" || !validExecutorAddressScheme(parsed.Scheme) || parsed.User != nil {
		return store.ExecutorNode{}, &ValidationError{err: fmt.Errorf("address must be an absolute HTTP(S) or gRPC URL without userinfo")}
	}
	if input.TTLSeconds < 5 || input.TTLSeconds > 300 {
		return store.ExecutorNode{}, &ValidationError{err: fmt.Errorf("ttl_seconds must be between 5 and 300")}
	}
	labels, err := normalizeExecutorLabels(input.Labels)
	if err != nil {
		return store.ExecutorNode{}, &ValidationError{err: err}
	}
	return s.registry.RegisterExecutorNode(ctx, input.TenantID, input.GroupID, strings.TrimSpace(input.NodeID), strings.TrimRight(input.Address, "/"), time.Duration(input.TTLSeconds)*time.Second, labels)
}

func (s *ExecutorService) UnregisterNode(ctx context.Context, tenantID, groupID, nodeID string) error {
	if tenantID == "" || groupID == "" || strings.TrimSpace(nodeID) == "" {
		return &ValidationError{err: fmt.Errorf("tenant_id, group_id and node_id are required")}
	}
	return s.registry.UnregisterExecutorNode(ctx, tenantID, groupID, strings.TrimSpace(nodeID))
}

func (s *ExecutorService) ListNodes(ctx context.Context, tenantID, groupID string, liveOnly bool) ([]store.ExecutorNode, error) {
	return s.registry.ListExecutorNodes(ctx, tenantID, groupID, liveOnly)
}

func normalizeExecutorGroupModel(group store.ExecutorGroup) (store.ExecutorGroup, error) {
	if group.TenantID == "" || strings.TrimSpace(group.Name) == "" {
		return store.ExecutorGroup{}, errExecutorGroupRequired
	}
	if !validExecutorRouteStrategy(group.RouteStrategy) {
		return store.ExecutorGroup{}, fmt.Errorf("unsupported route_strategy")
	}
	group.Name = strings.TrimSpace(group.Name)
	group.RegistrationMode = strings.ToLower(strings.TrimSpace(group.RegistrationMode))
	if group.RegistrationMode == "" {
		group.RegistrationMode = "automatic"
	}
	if group.RegistrationMode != "automatic" && group.RegistrationMode != "manual" {
		return store.ExecutorGroup{}, fmt.Errorf("registration_mode must be automatic or manual")
	}
	seen := make(map[string]struct{}, len(group.ManualAddresses))
	addresses := make([]string, 0, len(group.ManualAddresses))
	for _, raw := range group.ManualAddresses {
		address := strings.TrimRight(strings.TrimSpace(raw), "/")
		parsed, err := url.ParseRequestURI(address)
		if err != nil || parsed.Host == "" || !validExecutorAddressScheme(parsed.Scheme) || parsed.User != nil {
			return store.ExecutorGroup{}, fmt.Errorf("manual addresses must be absolute HTTP(S) or gRPC URLs without userinfo")
		}
		if _, exists := seen[address]; !exists {
			seen[address] = struct{}{}
			addresses = append(addresses, address)
		}
	}
	sort.Strings(addresses)
	group.ManualAddresses = addresses
	if group.RegistrationMode == "manual" && len(addresses) == 0 {
		return store.ExecutorGroup{}, fmt.Errorf("manual registration requires at least one address")
	}
	if group.RegistrationMode == "automatic" && len(addresses) != 0 {
		return store.ExecutorGroup{}, fmt.Errorf("automatic registration does not accept manual addresses")
	}
	return group, nil
}
