package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type apiKeySummary struct {
	ID, TenantID, Name, Role string
	CreatedAt                time.Time
	RevokedAt                *time.Time
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	resp, err := s.client.ListAPIKeys(r.Context(), &schedulerv1.ListAPIKeysRequest{TenantId: tenantID(r.Context())})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	keys := make([]apiKeySummary, 0, len(resp.GetApiKeys()))
	for _, key := range resp.GetApiKeys() {
		item := apiKeySummary{ID: key.GetId(), TenantID: key.GetTenantId(), Name: key.GetName(), Role: key.GetRole()}
		if key.CreatedAt != nil {
			item.CreatedAt = key.CreatedAt.AsTime()
		}
		if key.RevokedAt != nil {
			revoked := key.RevokedAt.AsTime()
			item.RevokedAt = &revoked
		}
		keys = append(keys, item)
	}
	writeJSON(w, 200, map[string]any{"api_keys": keys})
}
func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	var body struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Role != "owner" && body.Role != "admin" && body.Role != "developer" && body.Role != "viewer" {
		writeError(w, 400, "invalid role")
		return
	}
	if getPrincipal(r.Context()).Role != "owner" && (body.Role == "owner" || body.Role == "admin") {
		writeError(w, 403, "cannot grant a role equal to or above your own")
		return
	}
	resp, err := s.client.CreateAPIKey(r.Context(), &schedulerv1.CreateAPIKeyRequest{TenantId: tenantID(r.Context()), Name: body.Name, Role: body.Role})
	if err != nil {
		respond(w, nil, err, http.StatusCreated)
		return
	}
	key := resp.GetApiKey()
	createdAt := time.Time{}
	if key.GetCreatedAt() != nil {
		createdAt = key.GetCreatedAt().AsTime()
	}
	writeJSON(w, 201, map[string]any{"id": key.GetId(), "name": key.GetName(), "role": key.GetRole(), "token": resp.GetToken(), "created_at": createdAt})
}
func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	if _, err := s.client.RevokeAPIKey(r.Context(), &schedulerv1.RevokeAPIKeyRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id")}); err != nil {
		if status.Code(err) == codes.NotFound {
			writeError(w, 404, "API key not found")
			return
		}
		respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) listNotificationChannels(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	out, err := s.client.ListNotificationChannels(r.Context(), &schedulerv1.ListNotificationChannelsRequest{TenantId: tenantID(r.Context())})
	respond(w, out, err, http.StatusOK)
}

type notificationChannelRequest struct {
	Kind                  string          `json:"kind"`
	Name                  string          `json:"name"`
	Config                json.RawMessage `json:"config"`
	Events                []string        `json:"events"`
	AllJobs               *bool           `json:"all_jobs"`
	JobIDs                []string        `json:"job_ids"`
	MaxAttempts           int32           `json:"max_attempts"`
	BackoffInitialSeconds int32           `json:"backoff_initial_seconds"`
	BackoffMaxSeconds     int32           `json:"backoff_max_seconds"`
	Version               int64           `json:"version"`
}

func (s *Server) createNotificationChannel(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body notificationChannelRequest
	if !decode(w, r, &body) {
		return
	}
	allJobs := len(body.JobIDs) == 0
	if body.AllJobs != nil {
		allJobs = *body.AllJobs
	}
	out, err := s.client.CreateNotificationChannel(r.Context(), &schedulerv1.CreateNotificationChannelRequest{TenantId: tenantID(r.Context()), Kind: body.Kind, Name: body.Name, ConfigJson: body.Config, Events: body.Events, AllJobs: allJobs, JobIds: body.JobIDs, MaxAttempts: body.MaxAttempts, BackoffInitialSeconds: body.BackoffInitialSeconds, BackoffMaxSeconds: body.BackoffMaxSeconds})
	respond(w, out, err, http.StatusCreated)
}

func (s *Server) updateNotificationChannel(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body notificationChannelRequest
	if !decode(w, r, &body) {
		return
	}
	allJobs := len(body.JobIDs) == 0
	if body.AllJobs != nil {
		allJobs = *body.AllJobs
	}
	out, err := s.client.UpdateNotificationChannel(r.Context(), &schedulerv1.UpdateNotificationChannelRequest{Id: chi.URLParam(r, "id"), TenantId: tenantID(r.Context()), Kind: body.Kind, Name: body.Name, ConfigJson: body.Config, Events: body.Events, AllJobs: allJobs, JobIds: body.JobIDs, MaxAttempts: body.MaxAttempts, BackoffInitialSeconds: body.BackoffInitialSeconds, BackoffMaxSeconds: body.BackoffMaxSeconds, Version: body.Version})
	respond(w, out, err, http.StatusOK)
}

func (s *Server) setNotificationChannelEnabled(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Enabled bool  `json:"enabled"`
		Version int64 `json:"version"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.SetNotificationChannelEnabled(r.Context(), &schedulerv1.SetNotificationChannelEnabledRequest{Id: chi.URLParam(r, "id"), TenantId: tenantID(r.Context()), Enabled: body.Enabled, Version: body.Version})
	respond(w, out, err, http.StatusOK)
}

func (s *Server) deleteNotificationChannel(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	version, err := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	if err != nil || version < 1 {
		writeError(w, http.StatusBadRequest, "positive version is required")
		return
	}
	out, callErr := s.client.DeleteNotificationChannel(r.Context(), &schedulerv1.DeleteNotificationChannelRequest{Id: chi.URLParam(r, "id"), TenantId: tenantID(r.Context()), Version: version})
	respond(w, out, callErr, http.StatusOK)
}
func (s *Server) listNotificationHistory(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	limit, err := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)
	if r.URL.Query().Get("limit") == "" {
		limit = 100
	} else if err != nil {
		writeError(w, 400, "invalid limit")
		return
	}
	if limit < 1 || limit > 500 {
		writeError(w, 400, "limit must be between 1 and 500")
		return
	}
	out, callErr := s.client.ListNotificationHistory(r.Context(), &schedulerv1.ListNotificationHistoryRequest{TenantId: tenantID(r.Context()), ChannelId: r.URL.Query().Get("channel_id"), JobId: r.URL.Query().Get("job_id"), Status: r.URL.Query().Get("status"), Limit: parsedInt32(limit), Cursor: r.URL.Query().Get("cursor")})
	respond(w, out, callErr, http.StatusOK)
}

func parsedInt32(value int64) int32 {
	return int32(value) // #nosec G115 -- callers parse with bitSize 32 before conversion.
}
