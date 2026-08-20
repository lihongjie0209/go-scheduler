package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var job schedulerv1.Job
	if !decode(w, r, &job) {
		return
	}
	job.TenantId = tenantID(r.Context())
	applyDefaults(&job)
	out, err := s.client.CreateJob(r.Context(), &schedulerv1.CreateJobRequest{Job: &job})
	respond(w, out, err, 201)
}
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.GetJob(r.Context(), &schedulerv1.GetJobRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id")})
	respond(w, out, err, 200)
}
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.ListJobs(r.Context(), &schedulerv1.ListJobsRequest{TenantId: tenantID(r.Context()), Limit: 50})
	respond(w, out, err, 200)
}
func (s *Server) updateJob(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var job schedulerv1.Job
	if !decode(w, r, &job) {
		return
	}
	job.Id = chi.URLParam(r, "id")
	job.TenantId = tenantID(r.Context())
	if job.Headers == nil {
		existing, getErr := s.client.GetJob(r.Context(), &schedulerv1.GetJobRequest{TenantId: job.TenantId, Id: job.Id})
		if getErr != nil {
			respond(w, nil, getErr, http.StatusOK)
			return
		}
		job.Headers = existing.GetHeaders()
	}
	applyDefaults(&job)
	out, err := s.client.UpdateJob(r.Context(), &schedulerv1.UpdateJobRequest{Job: &job})
	respond(w, out, err, 200)
}
func (s *Server) listJobScriptVersions(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.ListJobScriptVersions(r.Context(), &schedulerv1.ListJobScriptVersionsRequest{TenantId: tenantID(r.Context()), JobId: chi.URLParam(r, "id")})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) rollbackJobScriptVersion(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Version int64  `json:"version"`
		Remark  string `json:"remark"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.RollbackJobScriptVersion(r.Context(), &schedulerv1.RollbackJobScriptVersionRequest{TenantId: tenantID(r.Context()), JobId: chi.URLParam(r, "id"), VersionId: chi.URLParam(r, "versionID"), JobVersion: body.Version, Remark: body.Remark})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Version int64 `json:"version"`
	}
	if !decode(w, r, &body) {
		return
	}
	_, err := s.client.DeleteJob(r.Context(), &schedulerv1.DeleteJobRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id"), Version: body.Version})
	if err != nil {
		respond(w, nil, err, 0)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) startJob(w http.ResponseWriter, r *http.Request) { s.setJobEnabled(w, r, true) }
func (s *Server) stopJob(w http.ResponseWriter, r *http.Request)  { s.setJobEnabled(w, r, false) }
func (s *Server) setJobEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Version int64 `json:"version"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.SetJobEnabled(r.Context(), &schedulerv1.SetJobEnabledRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id"), Enabled: enabled, Version: body.Version})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) triggerJob(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Input             string   `json:"input"`
		IdempotencyKey    string   `json:"idempotency_key"`
		OverrideAddresses []string `json:"override_addresses"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.TriggerJob(r.Context(), &schedulerv1.TriggerJobRequest{TenantId: tenantID(r.Context()), JobId: chi.URLParam(r, "id"), Input: body.Input, IdempotencyKey: body.IdempotencyKey, OverrideAddresses: body.OverrideAddresses})
	respond(w, out, err, 202)
}
func (s *Server) previewSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ScheduleType       string `json:"schedule_type"`
		ScheduleExpression string `json:"schedule_expression"`
		Timezone           string `json:"timezone"`
		After              string `json:"after"`
		Count              int32  `json:"count"`
	}
	if !decode(w, r, &body) {
		return
	}
	request := &schedulerv1.PreviewScheduleRequest{ScheduleType: body.ScheduleType, ScheduleExpression: body.ScheduleExpression, Timezone: body.Timezone, Count: body.Count}
	if body.After != "" {
		after, err := time.Parse(time.RFC3339, body.After)
		if err != nil {
			writeError(w, http.StatusBadRequest, "after must be RFC3339")
			return
		}
		request.After = timestamppb.New(after)
	}
	out, err := s.client.PreviewSchedule(r.Context(), request)
	respond(w, out, err, http.StatusOK)
}
func (s *Server) getJobDependencies(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.GetJobDependencies(r.Context(), &schedulerv1.GetJobDependenciesRequest{TenantId: tenantID(r.Context()), ParentJobId: chi.URLParam(r, "id")})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) setJobDependencies(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		ChildJobIDs []string `json:"child_job_ids"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.SetJobDependencies(r.Context(), &schedulerv1.SetJobDependenciesRequest{TenantId: tenantID(r.Context()), ParentJobId: chi.URLParam(r, "id"), ChildJobIds: body.ChildJobIDs})
	respond(w, out, err, http.StatusOK)
}
