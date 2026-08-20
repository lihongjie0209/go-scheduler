package core

import (
	"fmt"
	"sort"
	"strings"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func fromProto(j *schedulerv1.Job) store.Job {
	out := store.Job{ID: j.Id, TenantID: j.TenantId, Name: j.Name, Description: j.Description, ScheduleType: j.ScheduleType, ScheduleExpression: j.ScheduleExpression, Timezone: j.Timezone, TargetURL: j.TargetUrl, HTTPMethod: j.HttpMethod, Headers: j.Headers, BodyTemplate: j.BodyTemplate, TimeoutSeconds: j.TimeoutSeconds, MaxRetries: j.MaxRetries, OverlapPolicy: j.OverlapPolicy, MisfirePolicy: j.MisfirePolicy, Enabled: j.Enabled, Version: j.Version, MaxConcurrentRuns: j.MaxConcurrentRuns, MaxCatchUp: j.MaxCatchUp, CallbackTimeoutSeconds: j.CallbackTimeoutSeconds, MaxQueueSize: j.MaxQueueSize, ExecutorGroupID: j.ExecutorGroupId, ExecutorHandler: j.ExecutorHandler, ScriptLanguage: j.ScriptLanguage, ScriptSource: j.ScriptSource, RequiredExecutorLabels: j.RequiredExecutorLabels, ExcludedExecutorLabels: j.ExcludedExecutorLabels, KubernetesClusterID: j.KubernetesClusterId}
	if auth := j.GetDockerRegistryAuth(); auth != nil {
		out.DockerRegistryAuth = store.DockerRegistryAuth{Server: strings.TrimSpace(auth.GetServer()), Username: strings.TrimSpace(auth.GetUsername()), Password: auth.GetPassword(), Configured: auth.GetConfigured()}
	}
	return out
}

func validateJob(j *schedulerv1.Job) error {
	job := fromProto(j)
	if err := validateJobModel(&job, j.GetDockerRegistryAuth() != nil, j.GetClearDockerRegistryAuth()); err != nil {
		return err
	}
	j.RequiredExecutorLabels = job.RequiredExecutorLabels
	j.ExcludedExecutorLabels = job.ExcludedExecutorLabels
	return nil
}
func toProto(j store.Job) *schedulerv1.Job {
	out := &schedulerv1.Job{Id: j.ID, TenantId: j.TenantID, Name: j.Name, Description: j.Description, ScheduleType: j.ScheduleType, ScheduleExpression: j.ScheduleExpression, Timezone: j.Timezone, TargetUrl: j.TargetURL, HttpMethod: j.HTTPMethod, Headers: j.Headers, BodyTemplate: j.BodyTemplate, TimeoutSeconds: j.TimeoutSeconds, MaxRetries: j.MaxRetries, OverlapPolicy: j.OverlapPolicy, MisfirePolicy: j.MisfirePolicy, Enabled: j.Enabled, Version: j.Version, MaxConcurrentRuns: j.MaxConcurrentRuns, MaxCatchUp: j.MaxCatchUp, CallbackTimeoutSeconds: j.CallbackTimeoutSeconds, MaxQueueSize: j.MaxQueueSize, ExecutorGroupId: j.ExecutorGroupID, ExecutorHandler: j.ExecutorHandler, ScriptLanguage: j.ScriptLanguage, ScriptSource: j.ScriptSource, RequiredExecutorLabels: j.RequiredExecutorLabels, ExcludedExecutorLabels: j.ExcludedExecutorLabels, KubernetesClusterId: j.KubernetesClusterID}
	if j.NextRunAt != nil {
		out.NextRunAt = timestamppb.New(*j.NextRunAt)
	}
	if j.DockerRegistryAuth.Configured {
		out.DockerRegistryAuth = &schedulerv1.DockerRegistryAuth{Server: j.DockerRegistryAuth.Server, Username: j.DockerRegistryAuth.Username, Configured: true}
	}
	return out
}
func runToProto(r store.Run) *schedulerv1.Run {
	out := &schedulerv1.Run{Id: r.ID, TenantId: r.TenantID, JobId: r.JobID, Status: r.Status, Attempt: r.Attempt, ScheduledAt: timestamppb.New(r.ScheduledAt), ResponseStatus: r.ResponseStatus, ErrorMessage: r.ErrorMessage, ParentRunId: r.ParentRunID, RetryOfRunId: r.RetryOfRunID, TriggerType: r.TriggerType, ExecutorNodeId: r.ExecutorNodeID, ExecutorAddress: r.ExecutorAddress, BroadcastGroupId: r.BroadcastGroupID, ShardIndex: r.ShardIndex, ShardTotal: r.ShardTotal, OverrideAddresses: r.OverrideAddresses, ExternalExecutionId: r.ExternalExecutionID}
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
	return &schedulerv1.ExecutorNode{GroupId: node.GroupID, NodeId: node.NodeID, Address: node.Address, ExpiresAt: timestamppb.New(node.ExpiresAt), UpdatedAt: timestamppb.New(node.UpdatedAt), Online: node.Static || node.ExpiresAt.After(time.Now()), Static: node.Static, Labels: node.Labels}
}

func normalizeExecutorLabels(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	labels := make([]string, 0, len(values))
	for _, value := range values {
		label := strings.ToLower(strings.TrimSpace(value))
		if label == "" || len(label) > 63 {
			return nil, fmt.Errorf("executor labels must contain 1 to 63 characters")
		}
		for index, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '.' && character != '_' && character != '-' || index == 0 && (character == '.' || character == '_' || character == '-') {
				return nil, fmt.Errorf("executor label %q has invalid characters", label)
			}
		}
		if _, exists := seen[label]; !exists {
			seen[label] = struct{}{}
			labels = append(labels, label)
		}
	}
	if len(labels) > 32 {
		return nil, fmt.Errorf("at most 32 executor labels are allowed")
	}
	sort.Strings(labels)
	return labels, nil
}
