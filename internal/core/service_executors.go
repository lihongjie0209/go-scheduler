package core

import (
	"context"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func (s *Service) CreateExecutorGroup(ctx context.Context, req *schedulerv1.CreateExecutorGroupRequest) (*schedulerv1.ExecutorGroup, error) {
	created, err := s.executors.CreateGroup(ctx, executorGroupFromProto(req.GetGroup()))
	if err != nil {
		return nil, toStatus(err)
	}
	return executorGroupToProto(created), nil
}

func (s *Service) UpdateExecutorGroup(ctx context.Context, req *schedulerv1.UpdateExecutorGroupRequest) (*schedulerv1.ExecutorGroup, error) {
	updated, err := s.executors.UpdateGroup(ctx, executorGroupFromProto(req.GetGroup()))
	if err != nil {
		return nil, toStatus(err)
	}
	return executorGroupToProto(updated), nil
}

func (s *Service) DeleteExecutorGroup(ctx context.Context, req *schedulerv1.DeleteExecutorGroupRequest) (*schedulerv1.DeleteExecutorGroupResponse, error) {
	if err := s.executors.DeleteGroup(ctx, req.GetTenantId(), req.GetId(), req.GetVersion()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteExecutorGroupResponse{}, nil
}

func normalizeExecutorGroup(group *schedulerv1.ExecutorGroup) (*schedulerv1.ExecutorGroup, error) {
	if group == nil {
		return nil, toStatus(&ValidationError{err: errExecutorGroupRequired})
	}
	normalized, err := normalizeExecutorGroupModel(executorGroupFromProto(group))
	if err != nil {
		return nil, toStatus(&ValidationError{err: err})
	}
	return executorGroupToProto(normalized), nil
}

func executorGroupFromProto(group *schedulerv1.ExecutorGroup) store.ExecutorGroup {
	if group == nil {
		return store.ExecutorGroup{}
	}
	return store.ExecutorGroup{ID: group.Id, TenantID: group.TenantId, Name: group.Name, RouteStrategy: group.RouteStrategy, RegistrationMode: group.RegistrationMode, ManualAddresses: group.ManualAddresses, Version: group.Version}
}

func validExecutorRouteStrategy(strategy string) bool {
	switch strategy {
	case "first", "last", "round", "random", "hash", "lfu", "lru", "failover", "busyover", "sharding_broadcast":
		return true
	default:
		return false
	}
}

func validExecutorAddressScheme(scheme string) bool {
	return scheme == "http" || scheme == "https" || scheme == "grpc"
}

func (s *Service) ListExecutorGroups(ctx context.Context, req *schedulerv1.ListExecutorGroupsRequest) (*schedulerv1.ListExecutorGroupsResponse, error) {
	groups, err := s.executors.ListGroups(ctx, req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListExecutorGroupsResponse{Groups: make([]*schedulerv1.ExecutorGroup, 0, len(groups))}
	for _, group := range groups {
		out.Groups = append(out.Groups, executorGroupToProto(group))
	}
	return out, nil
}

func (s *Service) RegisterExecutorNode(ctx context.Context, req *schedulerv1.RegisterExecutorNodeRequest) (*schedulerv1.ExecutorNode, error) {
	node, err := s.executors.RegisterNode(ctx, RegisterExecutorNodeInput{TenantID: req.GetTenantId(), GroupID: req.GetGroupId(), NodeID: req.GetNodeId(), Address: req.GetAddress(), TTLSeconds: req.GetTtlSeconds(), Labels: req.GetLabels()})
	if err != nil {
		return nil, toStatus(err)
	}
	return executorNodeToProto(node), nil
}

func (s *Service) UnregisterExecutorNode(ctx context.Context, req *schedulerv1.UnregisterExecutorNodeRequest) (*schedulerv1.UnregisterExecutorNodeResponse, error) {
	if err := s.executors.UnregisterNode(ctx, req.GetTenantId(), req.GetGroupId(), req.GetNodeId()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.UnregisterExecutorNodeResponse{}, nil
}

func (s *Service) ListExecutorNodes(ctx context.Context, req *schedulerv1.ListExecutorNodesRequest) (*schedulerv1.ListExecutorNodesResponse, error) {
	nodes, err := s.executors.ListNodes(ctx, req.GetTenantId(), req.GetGroupId(), req.GetLiveOnly())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListExecutorNodesResponse{Nodes: make([]*schedulerv1.ExecutorNode, 0, len(nodes))}
	for _, node := range nodes {
		out.Nodes = append(out.Nodes, executorNodeToProto(node))
	}
	return out, nil
}
