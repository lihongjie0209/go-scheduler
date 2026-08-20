package core

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/schedule"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type JobReader interface {
	GetJob(context.Context, string, string) (store.Job, error)
	ListJobs(context.Context, string, int) ([]store.Job, error)
	JobExecutorLabels(context.Context, string) ([]string, []string, error)
}

type JobWriter interface {
	CreateJob(context.Context, store.Job) (store.Job, error)
	UpdateJob(context.Context, store.Job) (store.Job, error)
	SetJobEnabled(context.Context, string, string, bool, int64) (store.Job, error)
	DeleteJob(context.Context, string, string, int64) error
}

type JobScriptRepository interface {
	ListJobScriptVersions(context.Context, string, string) ([]store.JobScriptVersion, error)
	RollbackJobScriptVersion(context.Context, string, string, string, int64, string) (store.Job, error)
}

type JobTriggerer interface {
	TriggerJobWithOptions(context.Context, string, string, string, string, store.TriggerOptions) (store.Run, error)
}

type JobDependencyRepository interface {
	SetJobDependencies(context.Context, string, string, []string) error
	JobDependencies(context.Context, string, string) ([]string, error)
}

// JobService coordinates job validation and persistence independently of gRPC.
type JobService struct {
	reader       JobReader
	writer       JobWriter
	scripts      JobScriptRepository
	trigger      JobTriggerer
	dependencies JobDependencyRepository
}

type CreateJobInput struct {
	Job                        store.Job
	DockerRegistryAuthProvided bool
}

type UpdateJobInput struct {
	Job                        store.Job
	DockerRegistryAuthProvided bool
	ClearDockerRegistryAuth    bool
}

type TriggerJobInput struct {
	TenantID          string
	JobID             string
	IdempotencyKey    string
	Input             string
	OverrideAddresses []string
}

type SchedulePreviewInput struct {
	ScheduleType       string
	ScheduleExpression string
	Timezone           string
	After              time.Time
	Count              int
}

type ValidationError struct {
	err error
}

func (e *ValidationError) Error() string { return e.err.Error() }
func (e *ValidationError) Unwrap() error { return e.err }

func NewJobService(reader JobReader, writer JobWriter, scripts JobScriptRepository, trigger JobTriggerer, dependencies JobDependencyRepository) *JobService {
	return &JobService{reader: reader, writer: writer, scripts: scripts, trigger: trigger, dependencies: dependencies}
}

func (s *JobService) Create(ctx context.Context, input CreateJobInput) (store.Job, error) {
	job := input.Job
	if err := validateJobModel(&job, input.DockerRegistryAuthProvided, false); err != nil {
		return store.Job{}, &ValidationError{err: err}
	}
	if job.DockerRegistryAuth.Configured && job.DockerRegistryAuth.Password == "" {
		return store.Job{}, &ValidationError{err: fmt.Errorf("docker registry password is required when creating credentials")}
	}
	return s.writer.CreateJob(ctx, job)
}

func (s *JobService) Get(ctx context.Context, tenantID, id string) (store.Job, error) {
	job, err := s.reader.GetJob(ctx, tenantID, id)
	if err != nil {
		return store.Job{}, err
	}
	job.RequiredExecutorLabels, job.ExcludedExecutorLabels, err = s.reader.JobExecutorLabels(ctx, job.ID)
	return job, err
}

func (s *JobService) List(ctx context.Context, tenantID string, limit int) ([]store.Job, error) {
	return s.reader.ListJobs(ctx, tenantID, limit)
}

func (s *JobService) Update(ctx context.Context, input UpdateJobInput) (store.Job, error) {
	job := input.Job
	if err := validateJobModel(&job, input.DockerRegistryAuthProvided, input.ClearDockerRegistryAuth); err != nil {
		return store.Job{}, &ValidationError{err: err}
	}
	if input.ClearDockerRegistryAuth {
		job.DockerRegistryAuth = store.DockerRegistryAuth{}
	} else if !input.DockerRegistryAuthProvided || job.DockerRegistryAuth.Configured && job.DockerRegistryAuth.Password == "" {
		current, err := s.reader.GetJob(ctx, job.TenantID, job.ID)
		if err != nil {
			return store.Job{}, err
		}
		if input.DockerRegistryAuthProvided && (!current.DockerRegistryAuth.Configured || current.DockerRegistryAuth.Server != job.DockerRegistryAuth.Server || current.DockerRegistryAuth.Username != job.DockerRegistryAuth.Username) {
			return store.Job{}, &ValidationError{err: fmt.Errorf("docker registry password is required when changing credentials")}
		}
		job.DockerRegistryAuth = current.DockerRegistryAuth
	}
	return s.writer.UpdateJob(ctx, job)
}

func (s *JobService) SetEnabled(ctx context.Context, tenantID, id string, enabled bool, version int64) (store.Job, error) {
	if tenantID == "" || id == "" || version < 1 {
		return store.Job{}, &ValidationError{err: fmt.Errorf("tenant_id, id and positive version are required")}
	}
	return s.writer.SetJobEnabled(ctx, tenantID, id, enabled, version)
}

func (s *JobService) Delete(ctx context.Context, tenantID, id string, version int64) error {
	return s.writer.DeleteJob(ctx, tenantID, id, version)
}

func (s *JobService) ListScriptVersions(ctx context.Context, tenantID, jobID string) ([]store.JobScriptVersion, error) {
	if tenantID == "" || jobID == "" {
		return nil, &ValidationError{err: fmt.Errorf("tenant_id and job_id are required")}
	}
	return s.scripts.ListJobScriptVersions(ctx, tenantID, jobID)
}

func (s *JobService) RollbackScriptVersion(ctx context.Context, tenantID, jobID, versionID string, jobVersion int64, remark string) (store.Job, error) {
	if tenantID == "" || jobID == "" || versionID == "" || jobVersion < 1 {
		return store.Job{}, &ValidationError{err: fmt.Errorf("tenant_id, job_id, version_id and positive job_version are required")}
	}
	if len(strings.TrimSpace(remark)) > 200 {
		return store.Job{}, &ValidationError{err: fmt.Errorf("remark must not exceed 200 characters")}
	}
	return s.scripts.RollbackJobScriptVersion(ctx, tenantID, jobID, versionID, jobVersion, remark)
}

func (s *JobService) Trigger(ctx context.Context, input TriggerJobInput) (store.Run, error) {
	if strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.JobID) == "" {
		return store.Run{}, &ValidationError{err: fmt.Errorf("tenant_id and job_id are required")}
	}
	if len(input.IdempotencyKey) > 200 {
		return store.Run{}, &ValidationError{err: fmt.Errorf("idempotency_key must not exceed 200 bytes")}
	}
	if len(input.Input) > 1<<20 {
		return store.Run{}, &ValidationError{err: fmt.Errorf("input must not exceed 1 MiB")}
	}
	addresses, err := normalizeOverrideAddresses(input.OverrideAddresses)
	if err != nil {
		return store.Run{}, &ValidationError{err: err}
	}
	return s.trigger.TriggerJobWithOptions(ctx, input.TenantID, input.JobID, input.IdempotencyKey, input.Input, store.TriggerOptions{OverrideAddresses: addresses})
}

func (s *JobService) PreviewSchedule(input SchedulePreviewInput, now time.Time) ([]time.Time, error) {
	if strings.TrimSpace(input.ScheduleType) == "" || strings.TrimSpace(input.ScheduleExpression) == "" {
		return nil, &ValidationError{err: fmt.Errorf("schedule_type and schedule_expression are required")}
	}
	if input.Timezone == "" {
		input.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(input.Timezone); err != nil {
		return nil, &ValidationError{err: fmt.Errorf("invalid timezone: %w", err)}
	}
	if input.Count == 0 {
		input.Count = 5
	}
	if input.Count < 1 || input.Count > 100 {
		return nil, &ValidationError{err: fmt.Errorf("count must be between 1 and 100")}
	}
	if input.After.IsZero() {
		input.After = now
	}
	times, err := schedule.Preview(input.ScheduleType, input.ScheduleExpression, input.Timezone, input.After, input.Count)
	if err != nil {
		return nil, &ValidationError{err: err}
	}
	return times, nil
}

func (s *JobService) SetDependencies(ctx context.Context, tenantID, parentJobID string, childJobIDs []string) ([]string, error) {
	if tenantID == "" || parentJobID == "" {
		return nil, &ValidationError{err: fmt.Errorf("tenant_id and parent_job_id are required")}
	}
	ids, err := normalizeDependencyIDs(childJobIDs)
	if err != nil {
		return nil, &ValidationError{err: err}
	}
	for _, id := range ids {
		if id == parentJobID {
			return nil, &ValidationError{err: fmt.Errorf("job cannot depend on itself")}
		}
	}
	if err = s.dependencies.SetJobDependencies(ctx, tenantID, parentJobID, ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *JobService) GetDependencies(ctx context.Context, tenantID, parentJobID string) ([]string, error) {
	return s.dependencies.JobDependencies(ctx, tenantID, parentJobID)
}

func normalizeOverrideAddresses(addresses []string) ([]string, error) {
	if len(addresses) > 100 {
		return nil, fmt.Errorf("override_addresses must contain at most 100 addresses")
	}
	seen := make(map[string]struct{}, len(addresses))
	normalized := make([]string, 0, len(addresses))
	for _, raw := range addresses {
		address := strings.TrimRight(strings.TrimSpace(raw), "/")
		parsed, err := url.ParseRequestURI(address)
		if err != nil || parsed.Host == "" || !validExecutorAddressScheme(parsed.Scheme) || parsed.User != nil {
			return nil, fmt.Errorf("override addresses must be absolute HTTP(S) or gRPC URLs without userinfo")
		}
		if _, exists := seen[address]; !exists {
			seen[address] = struct{}{}
			normalized = append(normalized, address)
		}
	}
	sort.Strings(normalized)
	return normalized, nil
}

func validateJobModel(job *store.Job, hasDockerRegistryAuth, clearDockerRegistryAuth bool) error {
	if strings.TrimSpace(job.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if job.TenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if strings.TrimSpace(job.ExecutorGroupID) == "" {
		return fmt.Errorf("executor_group_id is required")
	}
	if strings.TrimSpace(job.ExecutorHandler) == "" {
		return fmt.Errorf("executor_handler is required")
	}
	if job.TargetURL != "" {
		parsed, err := url.ParseRequestURI(job.TargetURL)
		if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" {
			return fmt.Errorf("target_url must be an absolute HTTP or HTTPS URL")
		}
		if parsed.User != nil {
			return fmt.Errorf("target_url userinfo is not allowed")
		}
	}
	if job.ScriptLanguage != "" || job.ScriptSource != "" {
		expectedHandler := "__script__"
		switch job.ScriptLanguage {
		case "docker":
			expectedHandler = "__docker__"
		case "kubernetes":
			expectedHandler = "__kubernetes__"
		}
		if job.ExecutorGroupID == "" || job.ExecutorHandler != expectedHandler {
			return fmt.Errorf("%s jobs require executor_group_id and %s handler", job.ScriptLanguage, expectedHandler)
		}
		if job.ScriptLanguage != "shell" && job.ScriptLanguage != "python" && job.ScriptLanguage != "nodejs" && job.ScriptLanguage != "php" && job.ScriptLanguage != "powershell" && job.ScriptLanguage != "docker" && job.ScriptLanguage != "kubernetes" {
			return fmt.Errorf("script_language must be shell, python, nodejs, php, powershell, docker or kubernetes")
		}
		if job.ScriptSource == "" || len(job.ScriptSource) > 1<<20 {
			return fmt.Errorf("script_source must be between 1 byte and 1 MiB")
		}
	}
	if job.ScriptLanguage == "kubernetes" && job.KubernetesClusterID == "" {
		return fmt.Errorf("kubernetes_cluster_id is required for kubernetes jobs")
	}
	if job.ScriptLanguage != "kubernetes" && job.KubernetesClusterID != "" {
		return fmt.Errorf("kubernetes_cluster_id is only valid for kubernetes jobs")
	}
	if hasDockerRegistryAuth || clearDockerRegistryAuth {
		if job.ScriptLanguage != "docker" {
			return fmt.Errorf("docker registry credentials are only valid for docker jobs")
		}
		if hasDockerRegistryAuth && clearDockerRegistryAuth {
			return fmt.Errorf("docker_registry_auth and clear_docker_registry_auth are mutually exclusive")
		}
		if hasDockerRegistryAuth {
			auth := job.DockerRegistryAuth
			if !auth.Configured || strings.TrimSpace(auth.Server) == "" || strings.TrimSpace(auth.Username) == "" {
				return fmt.Errorf("configured docker registry credentials require server and username")
			}
			if len(auth.Server) > 512 || len(auth.Username) > 256 || len(auth.Password) > 4096 || strings.ContainsAny(auth.Server, " \t\r\n") || strings.ContainsAny(auth.Username, ":\r\n") {
				return fmt.Errorf("invalid docker registry credentials")
			}
		}
	}
	required, err := normalizeExecutorLabels(job.RequiredExecutorLabels)
	if err != nil {
		return fmt.Errorf("required_executor_labels: %w", err)
	}
	excluded, err := normalizeExecutorLabels(job.ExcludedExecutorLabels)
	if err != nil {
		return fmt.Errorf("excluded_executor_labels: %w", err)
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, label := range excluded {
		excludedSet[label] = struct{}{}
	}
	for _, label := range required {
		if _, exists := excludedSet[label]; exists {
			return fmt.Errorf("executor label %q cannot be both required and excluded", label)
		}
	}
	job.RequiredExecutorLabels, job.ExcludedExecutorLabels = required, excluded
	switch job.HTTPMethod {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return fmt.Errorf("unsupported HTTP method")
	}
	if strings.Contains(strings.ReplaceAll(job.BodyTemplate, "{{input}}", ""), "{{") {
		return fmt.Errorf("body_template only supports {{input}}")
	}
	if job.TimeoutSeconds < 1 || job.TimeoutSeconds > 3600 {
		return fmt.Errorf("timeout_seconds must be between 1 and 3600")
	}
	if job.MaxRetries < 0 || job.MaxRetries > 20 {
		return fmt.Errorf("max_retries must be between 0 and 20")
	}
	if job.OverlapPolicy != "serial" && job.OverlapPolicy != "discard_later" && job.OverlapPolicy != "cover_early" && job.OverlapPolicy != "parallel" && job.OverlapPolicy != "skip" && job.OverlapPolicy != "queue" {
		return fmt.Errorf("invalid overlap_policy")
	}
	if job.MisfirePolicy != "skip" && job.MisfirePolicy != "fire_once" && job.MisfirePolicy != "catch_up" {
		return fmt.Errorf("invalid misfire_policy")
	}
	if job.MaxConcurrentRuns < 1 {
		return fmt.Errorf("max_concurrent_runs must be positive")
	}
	if job.MaxCatchUp < 1 || job.MaxCatchUp > 1000 {
		return fmt.Errorf("max_catch_up must be between 1 and 1000")
	}
	if job.CallbackTimeoutSeconds < 1 || job.CallbackTimeoutSeconds > 86400 {
		return fmt.Errorf("callback_timeout_seconds must be between 1 and 86400")
	}
	if job.MaxQueueSize < 1 || job.MaxQueueSize > 100000 {
		return fmt.Errorf("max_queue_size must be between 1 and 100000")
	}
	if _, err := schedule.Next(job.ScheduleType, job.ScheduleExpression, job.Timezone, time.Now().UTC()); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}
	return nil
}
