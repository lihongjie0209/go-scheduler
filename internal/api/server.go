package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type principalKey struct{}

// dummyPasswordHash is public, deliberately non-matching Argon2 work used to
// equalize unknown-account login timing; it is not an authentication secret.
const dummyPasswordHash = "$argon2id$v=19$m=65536,t=3,p=2$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" //nolint:gosec

type principal struct {
	TenantID, UserID, Role string
	PlatformAdmin          bool
}
type Server struct {
	client       schedulerv1.SchedulerServiceClient
	store        *store.Store
	auth         *auth.Manager
	cookieSecure bool
	contextPath  string
	etcd         *clientv3.Client
	etcdPrefix   string
	instances    []map[string]any
	logins       *loginLimiter
}

func (s *Server) SetStandaloneInstance(instanceID string, startedAt time.Time) {
	s.instances = []map[string]any{{
		"service": "scheduler-server", "instance_id": instanceID,
		"version": "dev", "started_at": startedAt.UTC(), "draining": false,
	}}
}

func (s *Server) SetDiscovery(client *clientv3.Client, prefix string) {
	s.etcd = client
	s.etcdPrefix = prefix
}

func (s *Server) SetContextPath(contextPath string) { s.contextPath = contextPath }

func NewServer(client schedulerv1.SchedulerServiceClient, s *store.Store, manager *auth.Manager, cookieSecure ...bool) *Server {
	secure := true
	if len(cookieSecure) > 0 {
		secure = cookieSecure[0]
	}
	return &Server{client: client, store: s, auth: manager, cookieSecure: secure, logins: newLoginLimiter()}
}
func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(s.recoverer, s.requestLog)
	r.Get("/health/live", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	r.Get("/health/ready", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Post("/api/v1/callbacks/{runID}", s.completeCallback)
	r.Post("/api/v1/runs/{runID}/logs", s.appendRunLogs)
	r.Post("/api/v1/auth/login", s.login)
	r.Post("/api/v1/auth/refresh", s.refresh)
	r.Post("/api/v1/auth/logout", s.logout)
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(s.authenticate)
		r.Get("/auth/me", s.me)
		r.Get("/dashboard", s.dashboard)
		r.Get("/reports/runs", s.runReport)
		r.Post("/platform/users", s.createUser)
		r.Get("/platform/users", s.listUsers)
		r.Post("/platform/tenants", s.createTenant)
		r.Get("/platform/tenants", s.listTenants)
		r.Get("/platform/instances", s.listInstances)
		r.Put("/platform/tenants/{tenantID}/members/{userID}", s.putMembership)
		r.Get("/platform/tenants/{tenantID}/members", s.listMembers)
		r.Delete("/platform/tenants/{tenantID}/members/{userID}", s.deleteMembership)
		r.Patch("/platform/users/{id}", s.patchUser)
		r.Get("/jobs", s.listJobs)
		r.Post("/jobs", s.createJob)
		r.Get("/jobs/{id}", s.getJob)
		r.Put("/jobs/{id}", s.updateJob)
		r.Get("/jobs/{id}/script-versions", s.listJobScriptVersions)
		r.Post("/jobs/{id}/script-versions/{versionID}/rollback", s.rollbackJobScriptVersion)
		r.Post("/jobs/{id}/start", s.startJob)
		r.Post("/jobs/{id}/stop", s.stopJob)
		r.Delete("/jobs/{id}", s.deleteJob)
		r.Post("/jobs/{id}/trigger", s.triggerJob)
		r.Post("/schedules/preview", s.previewSchedule)
		r.Get("/jobs/{id}/dependencies", s.getJobDependencies)
		r.Put("/jobs/{id}/dependencies", s.setJobDependencies)
		r.Get("/runs", s.listRuns)
		r.Post("/runs/purge", s.purgeRunHistory)
		r.Get("/runs/{id}", s.getRun)
		r.Get("/runs/{id}/logs", s.listRunLogs)
		r.Post("/runs/{id}/cancel", s.cancelRun)
		r.Get("/executor-groups", s.listExecutorGroups)
		r.Post("/executor-groups", s.createExecutorGroup)
		r.Put("/executor-groups/{id}", s.updateExecutorGroup)
		r.Delete("/executor-groups/{id}", s.deleteExecutorGroup)
		r.Get("/executor-groups/{id}/nodes", s.listExecutorNodes)
		r.Put("/executor-groups/{id}/nodes/{nodeID}", s.registerExecutorNode)
		r.Delete("/executor-groups/{id}/nodes/{nodeID}", s.unregisterExecutorNode)
		r.Get("/kubernetes-clusters", s.listKubernetesClusters)
		r.Post("/kubernetes-clusters", s.createKubernetesCluster)
		r.Get("/kubernetes-clusters/{id}", s.getKubernetesCluster)
		r.Put("/kubernetes-clusters/{id}", s.updateKubernetesCluster)
		r.Delete("/kubernetes-clusters/{id}", s.deleteKubernetesCluster)
		r.Get("/notification-channels", s.listNotificationChannels)
		r.Post("/notification-channels", s.createNotificationChannel)
		r.Get("/api-keys", s.listAPIKeys)
		r.Post("/api-keys", s.createAPIKey)
		r.Delete("/api-keys/{id}", s.revokeAPIKey)
	})
	r.NotFound(http.NotFoundHandler().ServeHTTP)
	if s.contextPath == "" {
		return r
	}
	root := chi.NewRouter()
	root.Mount(s.contextPath, r)
	root.NotFound(http.NotFoundHandler().ServeHTTP)
	return root
}

type kubernetesClusterRequest struct {
	Name                  string `json:"name"`
	AuthMode              string `json:"auth_mode"`
	APIServer             string `json:"api_server"`
	Namespace             string `json:"namespace"`
	Kubeconfig            string `json:"kubeconfig"`
	Token                 string `json:"token"`
	CAData                string `json:"ca_data"`
	InsecureSkipTLSVerify bool   `json:"insecure_skip_tls_verify"`
	Version               int64  `json:"version"`
}

func publicKubernetesCluster(cluster store.KubernetesCluster) map[string]any {
	return map[string]any{"id": cluster.ID, "tenant_id": cluster.TenantID, "name": cluster.Name, "auth_mode": cluster.AuthMode, "api_server": cluster.APIServer, "namespace": cluster.Namespace, "insecure_skip_tls_verify": cluster.InsecureSkipTLSVerify, "credentials_configured": true, "version": cluster.Version, "created_at": cluster.CreatedAt, "updated_at": cluster.UpdatedAt}
}

func clusterFromRequest(tenant, id string, body kubernetesClusterRequest) store.KubernetesCluster {
	return store.KubernetesCluster{ID: id, TenantID: tenant, Name: body.Name, AuthMode: body.AuthMode, APIServer: body.APIServer, Namespace: body.Namespace, Credentials: store.KubernetesCredentials{Kubeconfig: body.Kubeconfig, Token: body.Token, CAData: body.CAData}, InsecureSkipTLSVerify: body.InsecureSkipTLSVerify, Version: body.Version}
}

func (s *Server) listKubernetesClusters(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	clusters, err := s.store.ListKubernetesClusters(r.Context(), tenantID(r.Context()))
	if err != nil {
		writeError(w, 500, "list kubernetes clusters failed")
		return
	}
	out := make([]map[string]any, 0, len(clusters))
	for _, cluster := range clusters {
		out = append(out, publicKubernetesCluster(cluster))
	}
	writeJSON(w, 200, map[string]any{"clusters": out})
}

func (s *Server) getKubernetesCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := s.store.GetKubernetesCluster(r.Context(), tenantID(r.Context()), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, 404, "kubernetes cluster not found")
		return
	}
	writeJSON(w, 200, publicKubernetesCluster(cluster))
}

func (s *Server) createKubernetesCluster(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	var body kubernetesClusterRequest
	if !decode(w, r, &body) {
		return
	}
	cluster, err := s.store.CreateKubernetesCluster(r.Context(), clusterFromRequest(tenantID(r.Context()), "", body))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, publicKubernetesCluster(cluster))
}

func (s *Server) updateKubernetesCluster(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	var body kubernetesClusterRequest
	if !decode(w, r, &body) {
		return
	}
	cluster, err := s.store.UpdateKubernetesCluster(r.Context(), clusterFromRequest(tenantID(r.Context()), chi.URLParam(r, "id"), body))
	if errors.Is(err, store.ErrConflict) {
		writeError(w, 409, "kubernetes cluster version conflict")
		return
	}
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, publicKubernetesCluster(cluster))
}

func (s *Server) deleteKubernetesCluster(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	version, err := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	if err != nil {
		writeError(w, 400, "version is required")
		return
	}
	err = s.store.DeleteKubernetesCluster(r.Context(), tenantID(r.Context()), chi.URLParam(r, "id"), version)
	if errors.Is(err, store.ErrKubernetesClusterInUse) {
		writeError(w, 409, "kubernetes cluster is referenced by a job")
		return
	}
	if err != nil {
		writeError(w, 409, "kubernetes cluster version conflict")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func requireTenantAdmin(w http.ResponseWriter, r *http.Request) bool {
	p := getPrincipal(r.Context())
	if p.TenantID == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return false
	}
	if p.Role != "owner" && p.Role != "admin" {
		writeError(w, 403, "tenant admin permission required")
		return false
	}
	return true
}
func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	keys, err := s.store.ListAPIKeys(r.Context(), tenantID(r.Context()))
	if err != nil {
		writeError(w, 500, "list API keys failed")
		return
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
	key, token, err := s.store.CreateAPIKey(r.Context(), tenantID(r.Context()), body.Name, body.Role)
	if err != nil {
		writeError(w, 500, "create API key failed")
		return
	}
	writeJSON(w, 201, map[string]any{"id": key.ID, "name": key.Name, "role": key.Role, "token": token, "created_at": key.CreatedAt})
}
func (s *Server) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	if !requireTenantAdmin(w, r) {
		return
	}
	if err := s.store.RevokeAPIKey(r.Context(), tenantID(r.Context()), chi.URLParam(r, "id")); err != nil {
		writeError(w, 404, "API key not found")
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
func (s *Server) createNotificationChannel(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Kind   string          `json:"kind"`
		Name   string          `json:"name"`
		Config json.RawMessage `json:"config"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.CreateNotificationChannel(r.Context(), &schedulerv1.CreateNotificationChannelRequest{TenantId: tenantID(r.Context()), Kind: body.Kind, Name: body.Name, ConfigJson: body.Config})
	respond(w, out, err, http.StatusCreated)
}
func (s *Server) completeCallback(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token     string `json:"token"`
		Succeeded bool   `json:"succeeded"`
		Message   string `json:"message"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.CompleteCallback(r.Context(), &schedulerv1.CompleteCallbackRequest{RunId: chi.URLParam(r, "runID"), Token: body.Token, Succeeded: body.Succeeded, Message: body.Message})
	respond(w, out, err, 200)
}
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || token == r.Header.Get("Authorization") {
			writeError(w, 401, "missing API key")
			return
		}
		var p principal
		if strings.HasPrefix(token, "gsk_") {
			tenant, role, err := s.store.AuthenticateAPIKey(r.Context(), token)
			if err != nil {
				writeError(w, 401, "invalid API key")
				return
			}
			p = principal{TenantID: tenant, Role: role}
		} else {
			claims, err := s.auth.Parse(token)
			if err != nil {
				writeError(w, 401, "invalid access token")
				return
			}
			user, userErr := s.store.GetUser(r.Context(), claims.Subject)
			if userErr != nil || user.Disabled {
				writeError(w, 401, "account unavailable")
				return
			}
			p = principal{UserID: claims.Subject, PlatformAdmin: claims.PlatformAdmin}
			if tenant := r.Header.Get("X-Tenant-ID"); tenant != "" {
				role, roleErr := s.store.GetMembershipRole(r.Context(), tenant, p.UserID)
				if roleErr != nil {
					writeError(w, 403, "tenant access denied")
					return
				}
				p.TenantID = tenant
				p.Role = role
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}
func getPrincipal(ctx context.Context) principal {
	v, _ := ctx.Value(principalKey{}).(principal)
	return v
}
func tenantID(ctx context.Context) string { return getPrincipal(ctx).TenantID }
func requireTenantWrite(w http.ResponseWriter, r *http.Request) bool {
	p := getPrincipal(r.Context())
	if p.TenantID == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return false
	}
	if p.Role != "owner" && p.Role != "admin" && p.Role != "developer" {
		writeError(w, 403, "write permission denied")
		return false
	}
	return true
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, 503, "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !decode(w, r, &body) {
		return
	}
	if !s.logins.allow(r.RemoteAddr, body.Email) {
		writeError(w, http.StatusTooManyRequests, "too many login attempts")
		return
	}
	user, err := s.store.GetUserByEmail(r.Context(), body.Email)
	passwordHash := user.PasswordHash
	if err != nil {
		passwordHash = dummyPasswordHash
	}
	passwordValid := auth.VerifyPassword(passwordHash, body.Password)
	if err != nil || user.Disabled || !passwordValid {
		writeError(w, 401, "invalid credentials")
		return
	}
	s.logins.reset(r.RemoteAddr, body.Email)
	token, err := s.auth.Issue(user.ID, user.PlatformAdmin)
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	refresh, _, err := s.store.CreateRefreshSession(r.Context(), user.ID, 7*24*time.Hour)
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	s.setRefreshCookie(w, refresh, 7*24*time.Hour)
	writeJSON(w, 200, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 900})
}

func (s *Server) setRefreshCookie(w http.ResponseWriter, value string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{Name: "scheduler_refresh", Value: value, Path: s.contextPath + "/api/v1/auth", MaxAge: int(ttl.Seconds()), HttpOnly: true, Secure: s.cookieSecure, SameSite: http.SameSiteStrictMode})
}
func (s *Server) clearRefreshCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: "scheduler_refresh", Path: s.contextPath + "/api/v1/auth", MaxAge: -1, HttpOnly: true, Secure: s.cookieSecure, SameSite: http.SameSiteStrictMode})
}
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("scheduler_refresh")
	if err != nil {
		writeError(w, 401, "refresh session required")
		return
	}
	raw, session, err := s.store.RotateRefreshSession(r.Context(), cookie.Value, 7*24*time.Hour)
	if err != nil {
		s.clearRefreshCookie(w)
		writeError(w, 401, "refresh session expired")
		return
	}
	user, err := s.store.GetUser(r.Context(), session.UserID)
	if err != nil || user.Disabled {
		s.clearRefreshCookie(w)
		writeError(w, 401, "account unavailable")
		return
	}
	token, err := s.auth.Issue(user.ID, user.PlatformAdmin)
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	s.setRefreshCookie(w, raw, 7*24*time.Hour)
	writeJSON(w, 200, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 900})
}
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("scheduler_refresh"); err == nil {
		_ = s.store.RevokeRefreshSession(r.Context(), c.Value)
	}
	s.clearRefreshCookie(w)
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	p := getPrincipal(r.Context())
	u, err := s.store.GetUser(r.Context(), p.UserID)
	if err != nil {
		writeError(w, 404, "user not found")
		return
	}
	tenants, err := s.store.UserTenants(r.Context(), u.ID, u.PlatformAdmin)
	if err != nil {
		writeError(w, 500, "list tenants failed")
		return
	}
	writeJSON(w, 200, map[string]any{"id": u.ID, "email": u.Email, "platform_admin": u.PlatformAdmin, "tenants": tenants})
}
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	d, err := s.store.Dashboard(r.Context(), tenantID(r.Context()))
	if err != nil {
		writeError(w, 500, "load dashboard failed")
		return
	}
	failures := make([]map[string]any, 0, len(d.RecentFailures))
	for _, run := range d.RecentFailures {
		failures = append(failures, map[string]any{"id": run.ID, "job_id": run.JobID, "status": run.Status, "scheduled_at": run.ScheduledAt, "error_message": run.ErrorMessage})
	}
	upcoming := make([]map[string]any, 0, len(d.Upcoming))
	for _, job := range d.Upcoming {
		upcoming = append(upcoming, map[string]any{"id": job.ID, "name": job.Name, "next_run_at": job.NextRunAt})
	}
	writeJSON(w, 200, map[string]any{"Jobs": d.Jobs, "EnabledJobs": d.EnabledJobs, "PendingRuns": d.PendingRuns, "RunningRuns": d.RunningRuns, "Succeeded24H": d.Succeeded24H, "Failed24H": d.Failed24H, "RecentFailures": failures, "Upcoming": upcoming})
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
func requirePlatformAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !getPrincipal(r.Context()).PlatformAdmin {
		writeError(w, 403, "platform admin required")
		return false
	}
	return true
}
func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	x, err := s.store.ListUsers(r.Context())
	if err != nil {
		writeError(w, 500, "list users failed")
		return
	}
	writeJSON(w, 200, map[string]any{"users": x})
}
func (s *Server) patchUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	var body struct {
		Disabled bool `json:"disabled"`
	}
	if !decode(w, r, &body) {
		return
	}
	if err := s.store.SetUserDisabled(r.Context(), chi.URLParam(r, "id"), body.Disabled); err != nil {
		writeError(w, 500, "update user failed")
		return
	}
	w.WriteHeader(204)
}
func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	x, err := s.store.ListTenants(r.Context())
	if err != nil {
		writeError(w, 500, "list tenants failed")
		return
	}
	writeJSON(w, 200, map[string]any{"tenants": x})
}
func (s *Server) listInstances(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	if s.etcd == nil {
		writeJSON(w, 200, map[string]any{"instances": s.instances})
		return
	}
	response, err := s.etcd.Get(r.Context(), s.etcdPrefix+"/", clientv3.WithPrefix())
	if err != nil {
		writeError(w, 503, "service discovery unavailable")
		return
	}
	instances := make([]map[string]any, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		var metadata discovery.Metadata
		if json.Unmarshal(kv.Value, &metadata) != nil {
			continue
		}
		parts := strings.Split(strings.Trim(string(kv.Key), "/"), "/")
		service := parts[len(parts)-2]
		instances = append(instances, map[string]any{"service": service, "instance_id": metadata.InstanceID, "grpc_address": metadata.GRPCAddress, "http_address": metadata.HTTPAddress, "version": metadata.Version, "started_at": metadata.StartedAt, "draining": metadata.Draining})
	}
	writeJSON(w, 200, map[string]any{"instances": instances})
}
func (s *Server) listMembers(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	x, err := s.store.TenantMembers(r.Context(), chi.URLParam(r, "tenantID"))
	if err != nil {
		writeError(w, 500, "list members failed")
		return
	}
	writeJSON(w, 200, map[string]any{"members": x})
}
func (s *Server) deleteMembership(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	err := s.store.DeleteMembership(r.Context(), chi.URLParam(r, "tenantID"), chi.URLParam(r, "userID"))
	if errors.Is(err, store.ErrConflict) {
		writeError(w, 409, "cannot remove the last owner")
		return
	}
	if err != nil {
		writeError(w, 500, "remove member failed")
		return
	}
	w.WriteHeader(204)
}
func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	if !getPrincipal(r.Context()).PlatformAdmin {
		writeError(w, 403, "platform admin required")
		return
	}
	var body struct {
		Email         string `json:"email"`
		Password      string `json:"password"`
		PlatformAdmin bool   `json:"platform_admin"`
	}
	if !decode(w, r, &body) {
		return
	}
	hash, err := auth.HashPassword(body.Password)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	user, err := s.store.CreateUser(r.Context(), body.Email, hash, body.PlatformAdmin)
	if err != nil {
		writeError(w, 409, "user already exists")
		return
	}
	writeJSON(w, 201, map[string]any{"id": user.ID, "email": user.Email, "platform_admin": user.PlatformAdmin})
}
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	if !getPrincipal(r.Context()).PlatformAdmin {
		writeError(w, 403, "platform admin required")
		return
	}
	var body struct {
		Name        string `json:"name"`
		OwnerUserID string `json:"owner_user_id"`
	}
	if !decode(w, r, &body) {
		return
	}
	id, err := s.store.CreateTenant(r.Context(), body.Name)
	if err != nil {
		writeError(w, 500, "create tenant failed")
		return
	}
	if body.OwnerUserID != "" {
		if err = s.store.AddMembership(r.Context(), id, body.OwnerUserID, "owner"); err != nil {
			writeError(w, 500, "add owner failed")
			return
		}
	}
	writeJSON(w, 201, map[string]string{"id": id, "name": body.Name})
}
func (s *Server) putMembership(w http.ResponseWriter, r *http.Request) {
	if !getPrincipal(r.Context()).PlatformAdmin {
		writeError(w, 403, "platform admin required")
		return
	}
	var body struct {
		Role string `json:"role"`
	}
	if !decode(w, r, &body) {
		return
	}
	if body.Role != "owner" && body.Role != "admin" && body.Role != "developer" && body.Role != "viewer" {
		writeError(w, 400, "invalid role")
		return
	}
	if err := s.store.AddMembership(r.Context(), chi.URLParam(r, "tenantID"), chi.URLParam(r, "userID"), body.Role); err != nil {
		writeError(w, 500, "membership update failed")
		return
	}
	w.WriteHeader(204)
}
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
		existing, getErr := s.store.GetJob(r.Context(), job.TenantId, job.Id)
		if getErr != nil {
			writeError(w, 404, "resource not found")
			return
		}
		job.Headers = existing.Headers
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
func (s *Server) createExecutorGroup(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Name             string   `json:"name"`
		RouteStrategy    string   `json:"route_strategy"`
		RegistrationMode string   `json:"registration_mode"`
		ManualAddresses  []string `json:"manual_addresses"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.CreateExecutorGroup(r.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: tenantID(r.Context()), Name: body.Name, RouteStrategy: body.RouteStrategy, RegistrationMode: body.RegistrationMode, ManualAddresses: body.ManualAddresses}})
	respond(w, out, err, http.StatusCreated)
}
func (s *Server) updateExecutorGroup(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Name             string   `json:"name"`
		RouteStrategy    string   `json:"route_strategy"`
		RegistrationMode string   `json:"registration_mode"`
		ManualAddresses  []string `json:"manual_addresses"`
		Version          int64    `json:"version"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.UpdateExecutorGroup(r.Context(), &schedulerv1.UpdateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{Id: chi.URLParam(r, "id"), TenantId: tenantID(r.Context()), Name: body.Name, RouteStrategy: body.RouteStrategy, RegistrationMode: body.RegistrationMode, ManualAddresses: body.ManualAddresses, Version: body.Version}})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) deleteExecutorGroup(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	version, err := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	if err != nil || version < 1 {
		writeError(w, http.StatusBadRequest, "version must be a positive integer")
		return
	}
	_, rpcErr := s.client.DeleteExecutorGroup(r.Context(), &schedulerv1.DeleteExecutorGroupRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id"), Version: version})
	if rpcErr != nil {
		respond(w, nil, rpcErr, http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) listExecutorGroups(w http.ResponseWriter, r *http.Request) {
	out, err := s.client.ListExecutorGroups(r.Context(), &schedulerv1.ListExecutorGroupsRequest{TenantId: tenantID(r.Context())})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) registerExecutorNode(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	var body struct {
		Address    string   `json:"address"`
		TTLSeconds int32    `json:"ttl_seconds"`
		Labels     []string `json:"labels"`
	}
	if !decode(w, r, &body) {
		return
	}
	out, err := s.client.RegisterExecutorNode(r.Context(), &schedulerv1.RegisterExecutorNodeRequest{TenantId: tenantID(r.Context()), GroupId: chi.URLParam(r, "id"), NodeId: chi.URLParam(r, "nodeID"), Address: body.Address, TtlSeconds: body.TTLSeconds, Labels: body.Labels})
	respond(w, out, err, http.StatusOK)
}
func (s *Server) unregisterExecutorNode(w http.ResponseWriter, r *http.Request) {
	if !requireTenantWrite(w, r) {
		return
	}
	_, err := s.client.UnregisterExecutorNode(r.Context(), &schedulerv1.UnregisterExecutorNodeRequest{TenantId: tenantID(r.Context()), GroupId: chi.URLParam(r, "id"), NodeId: chi.URLParam(r, "nodeID")})
	if err != nil {
		respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) listExecutorNodes(w http.ResponseWriter, r *http.Request) {
	liveOnly := r.URL.Query().Get("live_only") != "false"
	out, err := s.client.ListExecutorNodes(r.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: tenantID(r.Context()), GroupId: chi.URLParam(r, "id"), LiveOnly: liveOnly})
	respond(w, out, err, http.StatusOK)
}
func applyDefaults(j *schedulerv1.Job) {
	if j.Timezone == "" {
		j.Timezone = "UTC"
	}
	if j.HttpMethod == "" {
		j.HttpMethod = "POST"
	}
	if j.TimeoutSeconds == 0 {
		j.TimeoutSeconds = 30
	}
	if j.OverlapPolicy == "" {
		j.OverlapPolicy = "serial"
	}
	if j.MisfirePolicy == "" {
		j.MisfirePolicy = "fire_once"
	}
	if j.MaxConcurrentRuns == 0 {
		j.MaxConcurrentRuns = 1
	}
	if j.MaxCatchUp == 0 {
		j.MaxCatchUp = 10
	}
	if j.CallbackTimeoutSeconds == 0 {
		j.CallbackTimeoutSeconds = 3600
	}
	if j.MaxQueueSize == 0 {
		j.MaxQueueSize = 1000
	}
}
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if message, ok := dst.(proto.Message); ok {
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			err = (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, message)
		}
		if err != nil {
			writeError(w, 400, "invalid JSON: "+err.Error())
			return false
		}
		return true
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	return true
}
func respond(w http.ResponseWriter, v any, err error, code int) {
	if err == nil {
		if message, ok := v.(proto.Message); ok {
			payload, marshalErr := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)
			if marshalErr != nil {
				writeError(w, 500, "internal error")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			if _, writeErr := w.Write(payload); writeErr != nil {
				slog.Error("write protobuf response", "error", writeErr)
			}
			return
		}
		writeJSON(w, code, v)
		return
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		writeError(w, 400, status.Convert(err).Message())
	case codes.NotFound:
		writeError(w, 404, "resource not found")
	case codes.Aborted:
		writeError(w, 409, "resource version conflict")
	case codes.FailedPrecondition:
		writeError(w, 409, status.Convert(err).Message())
	case codes.Unavailable:
		writeError(w, 503, "scheduler core unavailable")
	case codes.ResourceExhausted:
		writeError(w, http.StatusTooManyRequests, status.Convert(err).Message())
	default:
		writeError(w, 500, "internal error")
	}
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("panic recovered", "panic", recovered)
				writeError(w, 500, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(recorder, r)
		observability.HTTPDuration.WithLabelValues(r.Method).Observe(time.Since(start).Seconds())
		observability.HTTPRequests.WithLabelValues(r.Method, strconv.Itoa(recorder.status/100)+"xx").Inc()
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
