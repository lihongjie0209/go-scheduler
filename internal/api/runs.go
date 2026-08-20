package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	d, err := s.client.GetDashboard(r.Context(), &schedulerv1.GetDashboardRequest{TenantId: tenantID(r.Context())})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	failures := make([]map[string]any, 0, len(d.GetRecentFailures()))
	for _, run := range d.GetRecentFailures() {
		var scheduledAt time.Time
		if run.GetScheduledAt() != nil {
			scheduledAt = run.GetScheduledAt().AsTime()
		}
		failures = append(failures, map[string]any{"id": run.GetId(), "job_id": run.GetJobId(), "status": run.GetStatus(), "scheduled_at": scheduledAt, "error_message": run.GetErrorMessage()})
	}
	upcoming := make([]map[string]any, 0, len(d.GetUpcoming()))
	for _, job := range d.GetUpcoming() {
		var nextRunAt *time.Time
		if job.GetNextRunAt() != nil {
			value := job.GetNextRunAt().AsTime()
			nextRunAt = &value
		}
		upcoming = append(upcoming, map[string]any{"id": job.GetId(), "name": job.GetName(), "next_run_at": nextRunAt})
	}
	writeJSON(w, 200, map[string]any{"Jobs": d.GetJobs(), "EnabledJobs": d.GetEnabledJobs(), "PendingRuns": d.GetPendingRuns(), "RunningRuns": d.GetRunningRuns(), "Succeeded24H": d.GetSucceeded_24H(), "Failed24H": d.GetFailed_24H(), "RecentFailures": failures, "Upcoming": upcoming})
}
func (s *Server) runReport(w http.ResponseWriter, r *http.Request) {
	tenant := tenantID(r.Context())
	if tenant == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	report, err := s.client.GetRunReport(r.Context(), &schedulerv1.GetRunReportRequest{TenantId: tenant, FromDate: r.URL.Query().Get("from"), ToDate: r.URL.Query().Get("to"), Timezone: r.URL.Query().Get("timezone")})
	respond(w, report, err, 200)
}
func (s *Server) purgeRunHistory(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Before string `json:"before"`
		JobID  string `json:"job_id"`
		Limit  int32  `json:"limit"`
	}
	if !decode(w, r, &body) {
		return
	}
	before, err := time.Parse(time.RFC3339, body.Before)
	if err != nil {
		writeError(w, 400, "before must be an RFC3339 timestamp")
		return
	}
	response, err := s.client.PurgeRunHistory(r.Context(), &schedulerv1.PurgeRunHistoryRequest{TenantId: tenantID(r.Context()), JobId: body.JobID, Before: timestamppb.New(before), Limit: body.Limit})
	respond(w, response, err, 200)
}
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.ListRuns(r.Context(), &schedulerv1.ListRunsRequest{TenantId: tenantID(r.Context()), JobId: r.URL.Query().Get("job_id"), BroadcastGroupId: r.URL.Query().Get("broadcast_group_id"), Limit: 50})
	respond(w, out, err, 200)
}
func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.GetRun(r.Context(), &schedulerv1.GetRunRequest{TenantId: tenantID(r.Context()), RunId: chi.URLParam(r, "id")})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) appendRunLogs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token   string `json:"token"`
		Entries []struct {
			EntryID string `json:"entry_id"`
			Stream  string `json:"stream"`
			Content string `json:"content"`
		} `json:"entries"`
	}
	if !decode(w, r, &body) {
		return
	}
	entries := make([]*schedulerv1.RunLogInput, 0, len(body.Entries))
	for _, entry := range body.Entries {
		entries = append(entries, &schedulerv1.RunLogInput{EntryId: entry.EntryID, Stream: entry.Stream, Content: entry.Content})
	}
	out, err := s.client.AppendRunLogs(r.Context(), &schedulerv1.AppendRunLogsRequest{RunId: chi.URLParam(r, "runID"), Token: body.Token, Entries: entries})
	respond(w, out, err, http.StatusAccepted)
}
func (s *Server) listRunLogs(w http.ResponseWriter, r *http.Request) {
	after, err := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	if r.URL.Query().Get("after") == "" {
		after = 0
		err = nil
	}
	if err != nil || after < 0 {
		writeError(w, http.StatusBadRequest, "after must be a non-negative integer")
		return
	}
	limit, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)
	if r.URL.Query().Get("limit") == "" {
		limit = 100
		err = nil
	}
	if err != nil || limit < 1 || limit > 500 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
		return
	}
	out, rpcErr := s.client.ListRunLogs(r.Context(), &schedulerv1.ListRunLogsRequest{TenantId: tenantID(r.Context()), RunId: chi.URLParam(r, "id"), AfterCursor: after, Limit: int32(limit)})
	respond(w, out, rpcErr, http.StatusOK)
}
func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.CancelRun(r.Context(), &schedulerv1.CancelRunRequest{TenantId: tenantID(r.Context()), RunId: chi.URLParam(r, "id"), Reason: body.Reason})
	respond(w, out, err, http.StatusOK)
}
