package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) CreateJob(ctx context.Context, req *schedulerv1.CreateJobRequest) (*schedulerv1.Job, error) {
	if req.GetJob() == nil {
		return nil, status.Error(codes.InvalidArgument, "job is required")
	}
	j, err := s.jobs.Create(ctx, CreateJobInput{
		Job:                        fromProto(req.Job),
		DockerRegistryAuthProvided: req.Job.GetDockerRegistryAuth() != nil,
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) GetJob(ctx context.Context, req *schedulerv1.GetJobRequest) (*schedulerv1.Job, error) {
	j, err := s.jobs.Get(ctx, req.TenantId, req.Id)
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) ListJobs(ctx context.Context, req *schedulerv1.ListJobsRequest) (*schedulerv1.ListJobsResponse, error) {
	jobs, err := s.jobs.List(ctx, req.TenantId, int(req.Limit))
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
	j, err := s.jobs.Update(ctx, UpdateJobInput{
		Job:                        fromProto(req.Job),
		DockerRegistryAuthProvided: req.Job.GetDockerRegistryAuth() != nil,
		ClearDockerRegistryAuth:    req.Job.GetClearDockerRegistryAuth(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(j), nil
}
func (s *Service) ListJobScriptVersions(ctx context.Context, req *schedulerv1.ListJobScriptVersionsRequest) (*schedulerv1.ListJobScriptVersionsResponse, error) {
	versions, err := s.jobs.ListScriptVersions(ctx, req.GetTenantId(), req.GetJobId())
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
	job, err := s.jobs.RollbackScriptVersion(ctx, req.GetTenantId(), req.GetJobId(), req.GetVersionId(), req.GetJobVersion(), req.GetRemark())
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(job), nil
}
func (s *Service) SetJobEnabled(ctx context.Context, req *schedulerv1.SetJobEnabledRequest) (*schedulerv1.Job, error) {
	job, err := s.jobs.SetEnabled(ctx, req.GetTenantId(), req.GetId(), req.GetEnabled(), req.GetVersion())
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(job), nil
}
func (s *Service) DeleteJob(ctx context.Context, req *schedulerv1.DeleteJobRequest) (*schedulerv1.DeleteJobResponse, error) {
	if err := s.jobs.Delete(ctx, req.TenantId, req.Id, req.Version); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.DeleteJobResponse{}, nil
}
func (s *Service) TriggerJob(ctx context.Context, req *schedulerv1.TriggerJobRequest) (*schedulerv1.Run, error) {
	r, err := s.jobs.Trigger(ctx, TriggerJobInput{TenantID: req.GetTenantId(), JobID: req.GetJobId(), IdempotencyKey: req.GetIdempotencyKey(), Input: req.GetInput(), OverrideAddresses: req.GetOverrideAddresses()})
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(r), nil
}

func (s *Service) PreviewSchedule(_ context.Context, req *schedulerv1.PreviewScheduleRequest) (*schedulerv1.PreviewScheduleResponse, error) {
	input, err := schedulePreviewInput(req, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	times, err := s.jobs.PreviewSchedule(input, time.Now().UTC())
	if err != nil {
		return nil, toStatus(err)
	}
	response := &schedulerv1.PreviewScheduleResponse{TriggerTimes: make([]*timestamppb.Timestamp, 0, len(times))}
	for _, triggerTime := range times {
		response.TriggerTimes = append(response.TriggerTimes, timestamppb.New(triggerTime))
	}
	return response, nil
}

func normalizePreviewScheduleRequest(req *schedulerv1.PreviewScheduleRequest, now time.Time) (time.Time, int, error) {
	input, err := schedulePreviewInput(req, now)
	return input.After, input.Count, err
}

func schedulePreviewInput(req *schedulerv1.PreviewScheduleRequest, now time.Time) (SchedulePreviewInput, error) {
	if req == nil || strings.TrimSpace(req.GetScheduleType()) == "" || strings.TrimSpace(req.GetScheduleExpression()) == "" {
		return SchedulePreviewInput{}, fmt.Errorf("schedule_type and schedule_expression are required")
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(req.Timezone); err != nil {
		return SchedulePreviewInput{}, fmt.Errorf("invalid timezone: %w", err)
	}
	count := int(req.Count)
	if count == 0 {
		count = 5
	}
	if count < 1 || count > 100 {
		return SchedulePreviewInput{}, fmt.Errorf("count must be between 1 and 100")
	}
	after := now
	if req.After != nil {
		if err := req.After.CheckValid(); err != nil {
			return SchedulePreviewInput{}, fmt.Errorf("invalid after timestamp: %w", err)
		}
		after = req.After.AsTime()
	}
	return SchedulePreviewInput{ScheduleType: req.ScheduleType, ScheduleExpression: req.ScheduleExpression, Timezone: req.Timezone, After: after, Count: count}, nil
}

func normalizeTriggerOverrideAddresses(addresses []string) ([]string, error) {
	return normalizeOverrideAddresses(addresses)
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
