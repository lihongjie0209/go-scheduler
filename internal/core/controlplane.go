package core

import (
	"context"
	"errors"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) Ping(ctx context.Context, _ *schedulerv1.PingRequest) (*schedulerv1.PingResponse, error) {
	if err := s.store.Ping(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, "database unavailable")
	}
	return &schedulerv1.PingResponse{}, nil
}

func (s *Service) AuthenticateAPIKey(ctx context.Context, req *schedulerv1.AuthenticateAPIKeyRequest) (*schedulerv1.AuthenticateAPIKeyResponse, error) {
	tenantID, role, err := s.store.AuthenticateAPIKey(ctx, req.GetToken())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.AuthenticateAPIKeyResponse{TenantId: tenantID, Role: role}, nil
}

func (s *Service) GetUser(ctx context.Context, req *schedulerv1.GetUserRequest) (*schedulerv1.User, error) {
	user, err := s.store.GetUser(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return userToProto(user, time.Time{}), nil
}

func (s *Service) GetUserByEmail(ctx context.Context, req *schedulerv1.GetUserByEmailRequest) (*schedulerv1.User, error) {
	user, err := s.store.GetUserByEmail(ctx, req.GetEmail())
	if err != nil {
		return nil, toStatus(err)
	}
	return userToProto(user, time.Time{}), nil
}

func (s *Service) GetMembershipRole(ctx context.Context, req *schedulerv1.GetMembershipRoleRequest) (*schedulerv1.GetMembershipRoleResponse, error) {
	role, err := s.store.GetMembershipRole(ctx, req.GetTenantId(), req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.GetMembershipRoleResponse{Role: role}, nil
}

func (s *Service) ListUserTenants(ctx context.Context, req *schedulerv1.ListUserTenantsRequest) (*schedulerv1.ListUserTenantsResponse, error) {
	tenants, err := s.store.UserTenants(ctx, req.GetUserId(), req.GetPlatformAdmin())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListUserTenantsResponse{Tenants: make([]*schedulerv1.TenantAccess, 0, len(tenants))}
	for _, tenant := range tenants {
		out.Tenants = append(out.Tenants, &schedulerv1.TenantAccess{Id: tenant.ID, Name: tenant.Name, Role: tenant.Role, MaxConcurrentRuns: int32(tenant.MaxConcurrentRuns)}) // #nosec G115 -- tenant concurrency is stored as a small positive integer.
	}
	return out, nil
}

func (s *Service) CreateRefreshSession(ctx context.Context, req *schedulerv1.CreateRefreshSessionRequest) (*schedulerv1.CreateRefreshSessionResponse, error) {
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	token, session, err := s.store.CreateRefreshSession(ctx, req.GetUserId(), ttl)
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.CreateRefreshSessionResponse{Token: token, UserId: session.UserID}, nil
}

func (s *Service) RotateRefreshSession(ctx context.Context, req *schedulerv1.RotateRefreshSessionRequest) (*schedulerv1.RotateRefreshSessionResponse, error) {
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	token, session, err := s.store.RotateRefreshSession(ctx, req.GetToken(), ttl)
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.RotateRefreshSessionResponse{Token: token, UserId: session.UserID}, nil
}

func (s *Service) RevokeRefreshSession(ctx context.Context, req *schedulerv1.RevokeRefreshSessionRequest) (*schedulerv1.RevokeRefreshSessionResponse, error) {
	if err := s.store.RevokeRefreshSession(ctx, req.GetToken()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.RevokeRefreshSessionResponse{}, nil
}

func (s *Service) ListUsers(ctx context.Context, _ *schedulerv1.ListUsersRequest) (*schedulerv1.ListUsersResponse, error) {
	users, err := s.store.ListUsers(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListUsersResponse{Users: make([]*schedulerv1.User, 0, len(users))}
	for _, user := range users {
		out.Users = append(out.Users, &schedulerv1.User{Id: user.ID, Email: user.Email, PlatformAdmin: user.PlatformAdmin, Disabled: user.Disabled, CreatedAt: timestamppb.New(user.CreatedAt)})
	}
	return out, nil
}

func (s *Service) CreateUser(ctx context.Context, req *schedulerv1.CreateUserRequest) (*schedulerv1.User, error) {
	user, err := s.store.CreateUser(ctx, req.GetEmail(), req.GetPasswordHash(), req.GetPlatformAdmin())
	if err != nil {
		return nil, toStatus(err)
	}
	return userToProto(user, time.Time{}), nil
}

func (s *Service) SetUserDisabled(ctx context.Context, req *schedulerv1.SetUserDisabledRequest) (*schedulerv1.SetUserDisabledResponse, error) {
	if err := s.store.SetUserDisabled(ctx, req.GetId(), req.GetDisabled()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.SetUserDisabledResponse{}, nil
}

func (s *Service) ListTenants(ctx context.Context, _ *schedulerv1.ListTenantsRequest) (*schedulerv1.ListTenantsResponse, error) {
	tenants, err := s.store.ListTenants(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListTenantsResponse{Tenants: make([]*schedulerv1.Tenant, 0, len(tenants))}
	for _, tenant := range tenants {
		out.Tenants = append(out.Tenants, &schedulerv1.Tenant{Id: tenant.ID, Name: tenant.Name, MaxConcurrentRuns: int32(tenant.MaxConcurrentRuns), CreatedAt: timestamppb.New(tenant.CreatedAt)}) // #nosec G115 -- tenant concurrency is stored as a small positive integer.
	}
	return out, nil
}

func (s *Service) CreateTenant(ctx context.Context, req *schedulerv1.CreateTenantRequest) (*schedulerv1.Tenant, error) {
	id, err := s.store.CreateTenant(ctx, req.GetName())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.Tenant{Id: id, Name: req.GetName()}, nil
}

func (s *Service) ListTenantMembers(ctx context.Context, req *schedulerv1.ListTenantMembersRequest) (*schedulerv1.ListTenantMembersResponse, error) {
	members, err := s.store.TenantMembers(ctx, req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListTenantMembersResponse{Members: make([]*schedulerv1.TenantMember, 0, len(members))}
	for _, member := range members {
		out.Members = append(out.Members, &schedulerv1.TenantMember{UserId: member.UserID, Email: member.Email, Role: member.Role, Disabled: member.Disabled})
	}
	return out, nil
}

func (s *Service) AddMembership(ctx context.Context, req *schedulerv1.AddMembershipRequest) (*schedulerv1.AddMembershipResponse, error) {
	if err := s.store.AddMembership(ctx, req.GetTenantId(), req.GetUserId(), req.GetRole()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.AddMembershipResponse{}, nil
}

func (s *Service) DeleteMembership(ctx context.Context, req *schedulerv1.DeleteMembershipRequest) (*schedulerv1.DeleteMembershipResponse, error) {
	if err := s.store.DeleteMembership(ctx, req.GetTenantId(), req.GetUserId()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteMembershipResponse{}, nil
}

func (s *Service) ListAPIKeys(ctx context.Context, req *schedulerv1.ListAPIKeysRequest) (*schedulerv1.ListAPIKeysResponse, error) {
	keys, err := s.store.ListAPIKeys(ctx, req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListAPIKeysResponse{ApiKeys: make([]*schedulerv1.APIKey, 0, len(keys))}
	for _, key := range keys {
		item := &schedulerv1.APIKey{Id: key.ID, TenantId: key.TenantID, Name: key.Name, Role: key.Role, CreatedAt: timestamppb.New(key.CreatedAt)}
		if key.RevokedAt != nil {
			item.RevokedAt = timestamppb.New(*key.RevokedAt)
		}
		out.ApiKeys = append(out.ApiKeys, item)
	}
	return out, nil
}

func (s *Service) CreateAPIKey(ctx context.Context, req *schedulerv1.CreateAPIKeyRequest) (*schedulerv1.CreateAPIKeyResponse, error) {
	key, token, err := s.store.CreateAPIKey(ctx, req.GetTenantId(), req.GetName(), req.GetRole())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.CreateAPIKeyResponse{ApiKey: &schedulerv1.APIKey{Id: key.ID, TenantId: key.TenantID, Name: key.Name, Role: key.Role, CreatedAt: timestamppb.New(key.CreatedAt)}, Token: token}, nil
}

func (s *Service) RevokeAPIKey(ctx context.Context, req *schedulerv1.RevokeAPIKeyRequest) (*schedulerv1.RevokeAPIKeyResponse, error) {
	if err := s.store.RevokeAPIKey(ctx, req.GetTenantId(), req.GetId()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.RevokeAPIKeyResponse{}, nil
}

func (s *Service) GetDashboard(ctx context.Context, req *schedulerv1.GetDashboardRequest) (*schedulerv1.Dashboard, error) {
	dashboard, err := s.store.Dashboard(ctx, req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.Dashboard{Jobs: dashboard.Jobs, EnabledJobs: dashboard.EnabledJobs, PendingRuns: dashboard.PendingRuns, RunningRuns: dashboard.RunningRuns, Succeeded_24H: dashboard.Succeeded24H, Failed_24H: dashboard.Failed24H}
	for _, run := range dashboard.RecentFailures {
		out.RecentFailures = append(out.RecentFailures, &schedulerv1.DashboardRun{Id: run.ID, JobId: run.JobID, Status: run.Status, ScheduledAt: timestamppb.New(run.ScheduledAt), ErrorMessage: run.ErrorMessage})
	}
	for _, job := range dashboard.Upcoming {
		item := &schedulerv1.DashboardJob{Id: job.ID, Name: job.Name}
		if job.NextRunAt != nil {
			item.NextRunAt = timestamppb.New(*job.NextRunAt)
		}
		out.Upcoming = append(out.Upcoming, item)
	}
	return out, nil
}

func (s *Service) ListKubernetesClusters(ctx context.Context, req *schedulerv1.ListKubernetesClustersRequest) (*schedulerv1.ListKubernetesClustersResponse, error) {
	clusters, err := s.store.ListKubernetesClusters(ctx, req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListKubernetesClustersResponse{Clusters: make([]*schedulerv1.KubernetesCluster, 0, len(clusters))}
	for _, cluster := range clusters {
		out.Clusters = append(out.Clusters, kubernetesClusterToProto(cluster, false))
	}
	return out, nil
}

func (s *Service) GetKubernetesCluster(ctx context.Context, req *schedulerv1.GetKubernetesClusterRequest) (*schedulerv1.KubernetesCluster, error) {
	cluster, err := s.store.GetKubernetesCluster(ctx, req.GetTenantId(), req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}
	return kubernetesClusterToProto(cluster, false), nil
}

func (s *Service) CreateKubernetesCluster(ctx context.Context, req *schedulerv1.CreateKubernetesClusterRequest) (*schedulerv1.KubernetesCluster, error) {
	if req.GetCluster() == nil {
		return nil, status.Error(codes.InvalidArgument, "cluster is required")
	}
	cluster, err := s.store.CreateKubernetesCluster(ctx, kubernetesClusterFromProto(req.GetCluster()))
	if err != nil {
		return nil, kubernetesWriteStatus(err)
	}
	return kubernetesClusterToProto(cluster, false), nil
}

func (s *Service) UpdateKubernetesCluster(ctx context.Context, req *schedulerv1.UpdateKubernetesClusterRequest) (*schedulerv1.KubernetesCluster, error) {
	if req.GetCluster() == nil {
		return nil, status.Error(codes.InvalidArgument, "cluster is required")
	}
	incoming := kubernetesClusterFromProto(req.GetCluster())
	if incoming.Credentials.Kubeconfig == "" && incoming.Credentials.Token == "" && incoming.Credentials.CAData == "" {
		current, err := s.store.GetKubernetesCluster(ctx, incoming.TenantID, incoming.ID)
		if err != nil {
			return nil, toStatus(err)
		}
		if current.AuthMode != incoming.AuthMode {
			return nil, status.Error(codes.InvalidArgument, "credentials are required when changing auth_mode")
		}
		incoming.Credentials = current.Credentials
	}
	cluster, err := s.store.UpdateKubernetesCluster(ctx, incoming)
	if err != nil {
		return nil, kubernetesWriteStatus(err)
	}
	return kubernetesClusterToProto(cluster, false), nil
}

func kubernetesWriteStatus(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrKubernetesClusterInUse):
		return toStatus(err)
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func (s *Service) DeleteKubernetesCluster(ctx context.Context, req *schedulerv1.DeleteKubernetesClusterRequest) (*schedulerv1.DeleteKubernetesClusterResponse, error) {
	if err := s.store.DeleteKubernetesCluster(ctx, req.GetTenantId(), req.GetId(), req.GetVersion()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteKubernetesClusterResponse{}, nil
}

func userToProto(user store.User, createdAt time.Time) *schedulerv1.User {
	out := &schedulerv1.User{Id: user.ID, Email: user.Email, PasswordHash: user.PasswordHash, PlatformAdmin: user.PlatformAdmin, Disabled: user.Disabled}
	if !createdAt.IsZero() {
		out.CreatedAt = timestamppb.New(createdAt)
	}
	return out
}

func kubernetesClusterFromProto(cluster *schedulerv1.KubernetesCluster) store.KubernetesCluster {
	return store.KubernetesCluster{
		ID: cluster.GetId(), TenantID: cluster.GetTenantId(), Name: cluster.GetName(), AuthMode: cluster.GetAuthMode(),
		APIServer: cluster.GetApiServer(), Namespace: cluster.GetNamespace(),
		Credentials:           store.KubernetesCredentials{Kubeconfig: cluster.GetKubeconfig(), Token: cluster.GetToken(), CAData: cluster.GetCaData()},
		InsecureSkipTLSVerify: cluster.GetInsecureSkipTlsVerify(), MaxConcurrentJobs: cluster.GetMaxConcurrentJobs(), Version: cluster.GetVersion(),
	}
}

func kubernetesClusterToProto(cluster store.KubernetesCluster, includeSecrets bool) *schedulerv1.KubernetesCluster {
	out := &schedulerv1.KubernetesCluster{
		Id: cluster.ID, TenantId: cluster.TenantID, Name: cluster.Name, AuthMode: cluster.AuthMode, ApiServer: cluster.APIServer,
		Namespace: cluster.Namespace, InsecureSkipTlsVerify: cluster.InsecureSkipTLSVerify, MaxConcurrentJobs: cluster.MaxConcurrentJobs,
		Version: cluster.Version, CreatedAt: timestamppb.New(cluster.CreatedAt), UpdatedAt: timestamppb.New(cluster.UpdatedAt),
	}
	if includeSecrets {
		out.Kubeconfig = cluster.Credentials.Kubeconfig
		out.Token = cluster.Credentials.Token
		out.CaData = cluster.Credentials.CAData
	}
	return out
}
