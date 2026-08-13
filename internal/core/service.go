package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	store            *store.Store
	executorRegistry ExecutorRegistry
}

func NewService(s *store.Store, registries ...ExecutorRegistry) *Service {
	registry := ExecutorRegistry(s)
	if len(registries) > 0 && registries[0] != nil {
		registry = registries[0]
	}
	return &Service{store: s, executorRegistry: registry}
}

func (s *Service) CreateJob(ctx context.Context, req *schedulerv1.CreateJobRequest) (*schedulerv1.Job, error) {
	if req.GetJob() == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	if err := validateJob(req.Job); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	j, err := s.store.CreateJob(ctx, fromProto(req.Job))
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) GetJob(ctx context.Context, req *schedulerv1.GetJobRequest) (*schedulerv1.Job, error) {
	j, err := s.store.GetJob(ctx, req.TenantId, req.Id)
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) ListJobs(ctx context.Context, req *schedulerv1.ListJobsRequest) (*schedulerv1.ListJobsResponse, error) {
	jobs, err := s.store.ListJobs(ctx, req.TenantId, int(req.Limit))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListJobsResponse{Jobs: make([]*schedulerv1.Job, 0, len(jobs))}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, toProto(j))
	}
	return out, nil
}
func (s *Service) UpdateJob(ctx context.Context, req *schedulerv1.UpdateJobRequest) (*schedulerv1.Job, error) {
	if req.GetJob() == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	if err := validateJob(req.Job); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	j, err := s.store.UpdateJob(ctx, fromProto(req.Job))
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) ListJobScriptVersions(ctx context.Context, req *schedulerv1.ListJobScriptVersionsRequest) (*schedulerv1.ListJobScriptVersionsResponse, error) {
	if req.GetTenantId() == "" || req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and job_id are required")
	}
	versions, err := s.store.ListJobScriptVersions(ctx, req.GetTenantId(), req.GetJobId())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListJobScriptVersionsResponse{Versions: make([]*schedulerv1.JobScriptVersion, 0, len(versions))}
	for _, version := range versions {
		out.Versions = append(out.Versions, jobScriptVersionToProto(version))
	}
	return out, nil
}
func (s *Service) RollbackJobScriptVersion(ctx context.Context, req *schedulerv1.RollbackJobScriptVersionRequest) (*schedulerv1.Job, error) {
	if req.GetTenantId() == "" || req.GetJobId() == "" || req.GetVersionId() == "" || req.GetJobVersion() < 1 {
		return nil, status.Error(codes.InvalidArgument, "tenant_id, job_id, version_id and positive job_version are required")
	}
	if len(strings.TrimSpace(req.GetRemark())) > 200 {
		return nil, status.Error(codes.InvalidArgument, "remark must not exceed 200 characters")
	}
	job, err := s.store.RollbackJobScriptVersion(ctx, req.GetTenantId(), req.GetJobId(), req.GetVersionId(), req.GetJobVersion(), req.GetRemark())
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(job), nil
}
func (s *Service) SetJobEnabled(ctx context.Context, req *schedulerv1.SetJobEnabledRequest) (*schedulerv1.Job, error) {
	if req.GetTenantId() == "" || req.GetId() == "" || req.GetVersion() < 1 {
		return nil, status.Error(codes.InvalidArgument, "tenant_id, id and positive version are required")
	}
	job, err := s.store.SetJobEnabled(ctx, req.GetTenantId(), req.GetId(), req.GetEnabled(), req.GetVersion())
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(job), nil
}
func validateJob(j *schedulerv1.Job) error {
	if strings.TrimSpace(j.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if j.TenantId == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if j.ExecutorGroupId == "" {
		parsed, err := url.ParseRequestURI(j.TargetUrl)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("target_url must be an absolute HTTP or HTTPS URL")
		}
		if parsed.User != nil {
			return fmt.Errorf("target_url userinfo is not allowed")
		}
	} else if strings.TrimSpace(j.ExecutorHandler) == "" {
		return fmt.Errorf("executor_handler is required with executor_group_id")
	}
	if j.ScriptLanguage != "" || j.ScriptSource != "" {
		if j.ExecutorGroupId == "" || j.ExecutorHandler != "__script__" {
			return fmt.Errorf("script jobs require executor_group_id and __script__ handler")
		}
		if j.ScriptLanguage != "shell" && j.ScriptLanguage != "python" && j.ScriptLanguage != "nodejs" && j.ScriptLanguage != "php" && j.ScriptLanguage != "powershell" {
			return fmt.Errorf("script_language must be shell, python, nodejs, php or powershell")
		}
		if j.ScriptSource == "" || len(j.ScriptSource) > 1<<20 {
			return fmt.Errorf("script_source must be between 1 byte and 1 MiB")
		}
	}
	switch j.HttpMethod {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return fmt.Errorf("unsupported HTTP method")
	}
	if strings.Contains(strings.ReplaceAll(j.BodyTemplate, "{{input}}", ""), "{{") {
		return fmt.Errorf("body_template only supports {{input}}")
	}
	if j.TimeoutSeconds < 1 || j.TimeoutSeconds > 3600 {
		return fmt.Errorf("timeout_seconds must be between 1 and 3600")
	}
	if j.MaxRetries < 0 || j.MaxRetries > 20 {
		return fmt.Errorf("max_retries must be between 0 and 20")
	}
	if j.OverlapPolicy != "serial" && j.OverlapPolicy != "discard_later" && j.OverlapPolicy != "cover_early" && j.OverlapPolicy != "parallel" && j.OverlapPolicy != "skip" && j.OverlapPolicy != "queue" {
		return fmt.Errorf("invalid overlap_policy")
	}
	if j.MisfirePolicy != "skip" && j.MisfirePolicy != "fire_once" && j.MisfirePolicy != "catch_up" {
		return fmt.Errorf("invalid misfire_policy")
	}
	if j.MaxConcurrentRuns < 1 {
		return fmt.Errorf("max_concurrent_runs must be positive")
	}
	if j.MaxCatchUp < 1 || j.MaxCatchUp > 1000 {
		return fmt.Errorf("max_catch_up must be between 1 and 1000")
	}
	if j.CallbackTimeoutSeconds < 1 || j.CallbackTimeoutSeconds > 86400 {
		return fmt.Errorf("callback_timeout_seconds must be between 1 and 86400")
	}
	if j.MaxQueueSize < 1 || j.MaxQueueSize > 100000 {
		return fmt.Errorf("max_queue_size must be between 1 and 100000")
	}
	if _, err := schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, time.Now().UTC()); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}
	return nil
}
func (s *Service) DeleteJob(ctx context.Context, req *schedulerv1.DeleteJobRequest) (*schedulerv1.DeleteJobResponse, error) {
	if err := s.store.DeleteJob(ctx, req.TenantId, req.Id, req.Version); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteJobResponse{}, nil
}
func (s *Service) TriggerJob(ctx context.Context, req *schedulerv1.TriggerJobRequest) (*schedulerv1.Run, error) {
	if err := validateTriggerRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	addresses, err := normalizeTriggerOverrideAddresses(req.OverrideAddresses)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	r, err := s.store.TriggerJobWithOptions(ctx, req.TenantId, req.JobId, req.IdempotencyKey, req.Input, store.TriggerOptions{OverrideAddresses: addresses})
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(r), nil
}

func (s *Service) PreviewSchedule(_ context.Context, req *schedulerv1.PreviewScheduleRequest) (*schedulerv1.PreviewScheduleResponse, error) {
	after, count, err := normalizePreviewScheduleRequest(req, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	times, err := schedule.Preview(req.ScheduleType, req.ScheduleExpression, req.Timezone, after, count)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &schedulerv1.PreviewScheduleResponse{TriggerTimes: make([]*timestamppb.Timestamp, 0, len(times))}
	for _, triggerTime := range times {
		response.TriggerTimes = append(response.TriggerTimes, timestamppb.New(triggerTime))
	}
	return response, nil
}

func normalizePreviewScheduleRequest(req *schedulerv1.PreviewScheduleRequest, now time.Time) (time.Time, int, error) {
	if req == nil || strings.TrimSpace(req.GetScheduleType()) == "" || strings.TrimSpace(req.GetScheduleExpression()) == "" {
		return time.Time{}, 0, fmt.Errorf("schedule_type and schedule_expression are required")
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(req.Timezone); err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid timezone: %w", err)
	}
	count := int(req.Count)
	if count == 0 {
		count = 5
	}
	if count < 1 || count > 100 {
		return time.Time{}, 0, fmt.Errorf("count must be between 1 and 100")
	}
	after := now
	if req.After != nil {
		if err := req.After.CheckValid(); err != nil {
			return time.Time{}, 0, fmt.Errorf("invalid after timestamp: %w", err)
		}
		after = req.After.AsTime()
	}
	return after, count, nil
}

func normalizeTriggerOverrideAddresses(addresses []string) ([]string, error) {
	if len(addresses) > 100 {
		return nil, fmt.Errorf("override_addresses must contain at most 100 addresses")
	}
	seen := make(map[string]struct{}, len(addresses))
	normalized := make([]string, 0, len(addresses))
	for _, raw := range addresses {
		address := strings.TrimRight(strings.TrimSpace(raw), "/")
		parsed, err := url.ParseRequestURI(address)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return nil, fmt.Errorf("override addresses must be absolute HTTP or HTTPS URLs without userinfo")
		}
		if _, exists := seen[address]; !exists {
			seen[address] = struct{}{}
			normalized = append(normalized, address)
		}
	}
	sort.Strings(normalized)
	return normalized, nil
}

func validateTriggerRequest(req *schedulerv1.TriggerJobRequest) error {
	if req == nil || strings.TrimSpace(req.GetTenantId()) == "" || strings.TrimSpace(req.GetJobId()) == "" {
		return fmt.Errorf("tenant_id and job_id are required")
	}
	if len(req.GetIdempotencyKey()) > 200 {
		return fmt.Errorf("idempotency_key must not exceed 200 bytes")
	}
	if len(req.GetInput()) > 1<<20 {
		return fmt.Errorf("input must not exceed 1 MiB")
	}
	return nil
}
func (s *Service) ListRuns(ctx context.Context, req *schedulerv1.ListRunsRequest) (*schedulerv1.ListRunsResponse, error) {
	runs, err := s.store.ListRunsFiltered(ctx, req.TenantId, req.JobId, req.BroadcastGroupId, int(req.Limit))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListRunsResponse{Runs: make([]*schedulerv1.Run, 0, len(runs))}
	for _, r := range runs {
		out.Runs = append(out.Runs, runToProto(r))
	}
	return out, nil
}

func (s *Service) GetRun(ctx context.Context, req *schedulerv1.GetRunRequest) (*schedulerv1.Run, error) {
	if req.GetTenantId() == "" || req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and run_id are required")
	}
	run, err := s.store.GetRun(ctx, req.GetTenantId(), req.GetRunId())
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(run), nil
}

func normalizeCancelReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "cancelled by operator"
	}
	if len(reason) > 500 {
		return "", fmt.Errorf("reason must not exceed 500 bytes")
	}
	return reason, nil
}

func (s *Service) CancelRun(ctx context.Context, req *schedulerv1.CancelRunRequest) (*schedulerv1.Run, error) {
	if req.GetTenantId() == "" || req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and run_id are required")
	}
	reason, err := normalizeCancelReason(req.GetReason())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	run, err := s.store.CancelRun(ctx, req.GetTenantId(), req.GetRunId(), reason)
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(run), nil
}

func normalizeDependencyIDs(ids []string) ([]string, error) {
	if len(ids) > 100 {
		return nil, fmt.Errorf("at most 100 child jobs are allowed")
	}
	unique := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("child job id must not be empty")
		}
		unique[id] = struct{}{}
	}
	out := make([]string, 0, len(unique))
	for id := range unique {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Service) SetJobDependencies(ctx context.Context, req *schedulerv1.SetJobDependenciesRequest) (*schedulerv1.JobDependencies, error) {
	if req.GetTenantId() == "" || req.GetParentJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and parent_job_id are required")
	}
	ids, err := normalizeDependencyIDs(req.GetChildJobIds())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for _, id := range ids {
		if id == req.GetParentJobId() {
			return nil, status.Error(codes.InvalidArgument, "job cannot depend on itself")
		}
	}
	if err = s.store.SetJobDependencies(ctx, req.GetTenantId(), req.GetParentJobId(), ids); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.JobDependencies{ParentJobId: req.GetParentJobId(), ChildJobIds: ids}, nil
}
func (s *Service) GetJobDependencies(ctx context.Context, req *schedulerv1.GetJobDependenciesRequest) (*schedulerv1.JobDependencies, error) {
	ids, err := s.store.JobDependencies(ctx, req.GetTenantId(), req.GetParentJobId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.JobDependencies{ParentJobId: req.GetParentJobId(), ChildJobIds: ids}, nil
}

func (s *Service) CompleteCallback(ctx context.Context, req *schedulerv1.CompleteCallbackRequest) (*schedulerv1.CompleteCallbackResponse, error) {
	if req.RunId == "" || req.Token == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id and token are required")
	}
	hash := sha256.Sum256([]byte(req.Token))
	if err := s.store.CompleteCallback(ctx, req.RunId, hash[:], req.Succeeded, req.Message); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.CompleteCallbackResponse{}, nil
}

func (s *Service) CreateExecutorGroup(ctx context.Context, req *schedulerv1.CreateExecutorGroupRequest) (*schedulerv1.ExecutorGroup, error) {
	group, err := normalizeExecutorGroup(req.GetGroup())
	if err != nil {
		return nil, err
	}
	created, err := s.store.CreateExecutorGroup(ctx, executorGroupFromProto(group))
	if err != nil {
		return nil, toStatus(err)
	}
	return executorGroupToProto(created), nil
}

func (s *Service) UpdateExecutorGroup(ctx context.Context, req *schedulerv1.UpdateExecutorGroupRequest) (*schedulerv1.ExecutorGroup, error) {
	group, err := normalizeExecutorGroup(req.GetGroup())
	if err != nil {
		return nil, err
	}
	if group.GetId() == "" || group.GetVersion() < 1 {
		return nil, status.Error(codes.InvalidArgument, "id and positive version are required")
	}
	updated, err := s.store.UpdateExecutorGroup(ctx, executorGroupFromProto(group))
	if err != nil {
		return nil, toStatus(err)
	}
	return executorGroupToProto(updated), nil
}

func (s *Service) DeleteExecutorGroup(ctx context.Context, req *schedulerv1.DeleteExecutorGroupRequest) (*schedulerv1.DeleteExecutorGroupResponse, error) {
	if req.GetTenantId() == "" || req.GetId() == "" || req.GetVersion() < 1 {
		return nil, status.Error(codes.InvalidArgument, "tenant_id, id and positive version are required")
	}
	if err := s.store.DeleteExecutorGroup(ctx, req.TenantId, req.Id, req.Version); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteExecutorGroupResponse{}, nil
}

func normalizeExecutorGroup(group *schedulerv1.ExecutorGroup) (*schedulerv1.ExecutorGroup, error) {
	if group == nil || group.GetTenantId() == "" || strings.TrimSpace(group.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and name are required")
	}
	if !validExecutorRouteStrategy(group.RouteStrategy) {
		return nil, status.Error(codes.InvalidArgument, "unsupported route_strategy")
	}
	normalized := proto.Clone(group).(*schedulerv1.ExecutorGroup)
	normalized.Name = strings.TrimSpace(group.Name)
	normalized.RegistrationMode = strings.ToLower(strings.TrimSpace(group.RegistrationMode))
	if normalized.RegistrationMode == "" {
		normalized.RegistrationMode = "automatic"
	}
	if normalized.RegistrationMode != "automatic" && normalized.RegistrationMode != "manual" {
		return nil, status.Error(codes.InvalidArgument, "registration_mode must be automatic or manual")
	}
	seen := make(map[string]struct{}, len(group.ManualAddresses))
	normalized.ManualAddresses = nil
	for _, raw := range group.ManualAddresses {
		address := strings.TrimRight(strings.TrimSpace(raw), "/")
		parsed, err := url.ParseRequestURI(address)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return nil, status.Error(codes.InvalidArgument, "manual addresses must be absolute HTTP or HTTPS URLs without userinfo")
		}
		if _, ok := seen[address]; !ok {
			seen[address] = struct{}{}
			normalized.ManualAddresses = append(normalized.ManualAddresses, address)
		}
	}
	sort.Strings(normalized.ManualAddresses)
	if normalized.RegistrationMode == "manual" && len(normalized.ManualAddresses) == 0 {
		return nil, status.Error(codes.InvalidArgument, "manual registration requires at least one address")
	}
	if normalized.RegistrationMode == "automatic" && len(normalized.ManualAddresses) != 0 {
		return nil, status.Error(codes.InvalidArgument, "automatic registration does not accept manual addresses")
	}
	return normalized, nil
}

func executorGroupFromProto(group *schedulerv1.ExecutorGroup) store.ExecutorGroup {
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

func (s *Service) ListExecutorGroups(ctx context.Context, req *schedulerv1.ListExecutorGroupsRequest) (*schedulerv1.ListExecutorGroupsResponse, error) {
	groups, err := s.store.ListExecutorGroups(ctx, req.GetTenantId())
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
	if req.GetTenantId() == "" || req.GetGroupId() == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id, group_id and node_id are required")
	}
	parsed, err := url.ParseRequestURI(req.GetAddress())
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return nil, status.Error(codes.InvalidArgument, "address must be an absolute HTTP or HTTPS URL without userinfo")
	}
	if req.GetTtlSeconds() < 5 || req.GetTtlSeconds() > 300 {
		return nil, status.Error(codes.InvalidArgument, "ttl_seconds must be between 5 and 300")
	}
	node, err := s.executorRegistry.RegisterExecutorNode(ctx, req.TenantId, req.GroupId, strings.TrimSpace(req.NodeId), strings.TrimRight(req.Address, "/"), time.Duration(req.TtlSeconds)*time.Second)
	if err != nil {
		return nil, toStatus(err)
	}
	return executorNodeToProto(node), nil
}

func (s *Service) UnregisterExecutorNode(ctx context.Context, req *schedulerv1.UnregisterExecutorNodeRequest) (*schedulerv1.UnregisterExecutorNodeResponse, error) {
	if req.GetTenantId() == "" || req.GetGroupId() == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id, group_id and node_id are required")
	}
	if err := s.executorRegistry.UnregisterExecutorNode(ctx, req.TenantId, req.GroupId, strings.TrimSpace(req.NodeId)); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.UnregisterExecutorNodeResponse{}, nil
}

func (s *Service) ListExecutorNodes(ctx context.Context, req *schedulerv1.ListExecutorNodesRequest) (*schedulerv1.ListExecutorNodesResponse, error) {
	nodes, err := s.executorRegistry.ListExecutorNodes(ctx, req.GetTenantId(), req.GetGroupId(), req.GetLiveOnly())
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListExecutorNodesResponse{Nodes: make([]*schedulerv1.ExecutorNode, 0, len(nodes))}
	for _, node := range nodes {
		out.Nodes = append(out.Nodes, executorNodeToProto(node))
	}
	return out, nil
}

func fromProto(j *schedulerv1.Job) store.Job {
	return store.Job{ID: j.Id, TenantID: j.TenantId, Name: j.Name, Description: j.Description, ScheduleType: j.ScheduleType, ScheduleExpression: j.ScheduleExpression, Timezone: j.Timezone, TargetURL: j.TargetUrl, HTTPMethod: j.HttpMethod, Headers: j.Headers, BodyTemplate: j.BodyTemplate, TimeoutSeconds: j.TimeoutSeconds, MaxRetries: j.MaxRetries, OverlapPolicy: j.OverlapPolicy, MisfirePolicy: j.MisfirePolicy, Enabled: j.Enabled, Version: j.Version, MaxConcurrentRuns: j.MaxConcurrentRuns, MaxCatchUp: j.MaxCatchUp, CallbackTimeoutSeconds: j.CallbackTimeoutSeconds, MaxQueueSize: j.MaxQueueSize, ExecutorGroupID: j.ExecutorGroupId, ExecutorHandler: j.ExecutorHandler, ScriptLanguage: j.ScriptLanguage, ScriptSource: j.ScriptSource}
}
func toProto(j store.Job) *schedulerv1.Job {
	out := &schedulerv1.Job{Id: j.ID, TenantId: j.TenantID, Name: j.Name, Description: j.Description, ScheduleType: j.ScheduleType, ScheduleExpression: j.ScheduleExpression, Timezone: j.Timezone, TargetUrl: j.TargetURL, HttpMethod: j.HTTPMethod, BodyTemplate: j.BodyTemplate, TimeoutSeconds: j.TimeoutSeconds, MaxRetries: j.MaxRetries, OverlapPolicy: j.OverlapPolicy, MisfirePolicy: j.MisfirePolicy, Enabled: j.Enabled, Version: j.Version, MaxConcurrentRuns: j.MaxConcurrentRuns, MaxCatchUp: j.MaxCatchUp, CallbackTimeoutSeconds: j.CallbackTimeoutSeconds, MaxQueueSize: j.MaxQueueSize, ExecutorGroupId: j.ExecutorGroupID, ExecutorHandler: j.ExecutorHandler, ScriptLanguage: j.ScriptLanguage, ScriptSource: j.ScriptSource}
	if j.NextRunAt != nil {
		out.NextRunAt = timestamppb.New(*j.NextRunAt)
	}
	return out
}
func runToProto(r store.Run) *schedulerv1.Run {
	out := &schedulerv1.Run{Id: r.ID, TenantId: r.TenantID, JobId: r.JobID, Status: r.Status, Attempt: r.Attempt, ScheduledAt: timestamppb.New(r.ScheduledAt), ResponseStatus: r.ResponseStatus, ErrorMessage: r.ErrorMessage, ParentRunId: r.ParentRunID, RetryOfRunId: r.RetryOfRunID, TriggerType: r.TriggerType, ExecutorNodeId: r.ExecutorNodeID, ExecutorAddress: r.ExecutorAddress, BroadcastGroupId: r.BroadcastGroupID, ShardIndex: r.ShardIndex, ShardTotal: r.ShardTotal, OverrideAddresses: r.OverrideAddresses}
	if r.StartedAt != nil {
		out.StartedAt = timestamppb.New(*r.StartedAt)
	}
	if r.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*r.FinishedAt)
	}
	return out
}
func executorGroupToProto(group store.ExecutorGroup) *schedulerv1.ExecutorGroup {
	return &schedulerv1.ExecutorGroup{Id: group.ID, TenantId: group.TenantID, Name: group.Name, RouteStrategy: group.RouteStrategy, Version: group.Version, RegistrationMode: group.RegistrationMode, ManualAddresses: group.ManualAddresses}
}
func jobScriptVersionToProto(version store.JobScriptVersion) *schedulerv1.JobScriptVersion {
	return &schedulerv1.JobScriptVersion{Id: version.ID, JobId: version.JobID, Revision: version.Revision, ScriptLanguage: version.ScriptLanguage, ScriptSource: version.ScriptSource, Remark: version.Remark, CreatedAt: timestamppb.New(version.CreatedAt)}
}
func executorNodeToProto(node store.ExecutorNode) *schedulerv1.ExecutorNode {
	return &schedulerv1.ExecutorNode{GroupId: node.GroupID, NodeId: node.NodeID, Address: node.Address, ExpiresAt: timestamppb.New(node.ExpiresAt), UpdatedAt: timestamppb.New(node.UpdatedAt), Online: node.Static || node.ExpiresAt.After(time.Now()), Static: node.Static}
}
func toStatus(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.Aborted, "resource version conflict")
	case errors.Is(err, store.ErrQueueFull):
		return status.Error(codes.ResourceExhausted, "job queue is full")
	case errors.Is(err, store.ErrNotCancellable):
		return status.Error(codes.FailedPrecondition, "run is already terminal")
	case errors.Is(err, store.ErrDependencyCycle):
		return status.Error(codes.FailedPrecondition, "job dependency would create a cycle")
	case errors.Is(err, store.ErrRegistrationMode):
		return status.Error(codes.FailedPrecondition, "executor group uses manual registration")
	case errors.Is(err, store.ErrExecutorGroupInUse):
		return status.Error(codes.FailedPrecondition, "executor group is still referenced by a job")
	case errors.Is(err, store.ErrOverrideRequiresExecutorGroup):
		return status.Error(codes.FailedPrecondition, "executor address override requires an executor group job")
	default:
		return status.Error(codes.Internal, fmt.Sprintf("operation failed: %v", err))
	}
}

var _ schedulerv1.SchedulerServiceServer = (*Service)(nil)
