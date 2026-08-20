package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type userSummary struct {
	ID, Email               string
	PlatformAdmin, Disabled bool
	CreatedAt               time.Time
}

type tenantSummary struct {
	ID, Name          string
	MaxConcurrentRuns int
	CreatedAt         time.Time
}

type memberSummary struct {
	UserID, Email, Role string
	Disabled            bool
}

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	resp, err := s.client.ListUsers(r.Context(), &schedulerv1.ListUsersRequest{})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	users := make([]userSummary, 0, len(resp.GetUsers()))
	for _, user := range resp.GetUsers() {
		item := userSummary{ID: user.GetId(), Email: user.GetEmail(), PlatformAdmin: user.GetPlatformAdmin(), Disabled: user.GetDisabled()}
		if user.CreatedAt != nil {
			item.CreatedAt = user.CreatedAt.AsTime()
		}
		users = append(users, item)
	}
	writeJSON(w, 200, map[string]any{"users": users})
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
	if _, err := s.client.SetUserDisabled(r.Context(), &schedulerv1.SetUserDisabledRequest{Id: chi.URLParam(r, "id"), Disabled: body.Disabled}); err != nil {
		respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	resp, err := s.client.ListTenants(r.Context(), &schedulerv1.ListTenantsRequest{})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	tenants := make([]tenantSummary, 0, len(resp.GetTenants()))
	for _, tenant := range resp.GetTenants() {
		item := tenantSummary{ID: tenant.GetId(), Name: tenant.GetName(), MaxConcurrentRuns: int(tenant.GetMaxConcurrentRuns())}
		if tenant.CreatedAt != nil {
			item.CreatedAt = tenant.CreatedAt.AsTime()
		}
		tenants = append(tenants, item)
	}
	writeJSON(w, 200, map[string]any{"tenants": tenants})
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
	resp, err := s.client.ListTenantMembers(r.Context(), &schedulerv1.ListTenantMembersRequest{TenantId: chi.URLParam(r, "tenantID")})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	members := make([]memberSummary, 0, len(resp.GetMembers()))
	for _, member := range resp.GetMembers() {
		members = append(members, memberSummary{UserID: member.GetUserId(), Email: member.GetEmail(), Role: member.GetRole(), Disabled: member.GetDisabled()})
	}
	writeJSON(w, 200, map[string]any{"members": members})
}
func (s *Server) deleteMembership(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	_, err := s.client.DeleteMembership(r.Context(), &schedulerv1.DeleteMembershipRequest{TenantId: chi.URLParam(r, "tenantID"), UserId: chi.URLParam(r, "userID")})
	if err != nil {
		if status.Code(err) == codes.Aborted {
			writeError(w, 409, "cannot remove the last owner")
			return
		}
		respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(204)
}
func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	if !canAdministerPlatform(getPrincipal(r.Context())) {
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
	user, err := s.client.CreateUser(r.Context(), &schedulerv1.CreateUserRequest{Email: body.Email, PasswordHash: hash, PlatformAdmin: body.PlatformAdmin})
	if err != nil {
		writeError(w, 409, "user already exists")
		return
	}
	writeJSON(w, 201, map[string]any{"id": user.GetId(), "email": user.GetEmail(), "platform_admin": user.GetPlatformAdmin()})
}
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	if !canAdministerPlatform(getPrincipal(r.Context())) {
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
	tenant, err := s.client.CreateTenant(r.Context(), &schedulerv1.CreateTenantRequest{Name: body.Name})
	if err != nil {
		respond(w, nil, err, http.StatusCreated)
		return
	}
	if body.OwnerUserID != "" {
		if _, err = s.client.AddMembership(r.Context(), &schedulerv1.AddMembershipRequest{TenantId: tenant.GetId(), UserId: body.OwnerUserID, Role: "owner"}); err != nil {
			respond(w, nil, err, http.StatusCreated)
			return
		}
	}
	writeJSON(w, 201, map[string]string{"id": tenant.GetId(), "name": body.Name})
}
func (s *Server) putMembership(w http.ResponseWriter, r *http.Request) {
	if !canAdministerPlatform(getPrincipal(r.Context())) {
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
	if _, err := s.client.AddMembership(r.Context(), &schedulerv1.AddMembershipRequest{TenantId: chi.URLParam(r, "tenantID"), UserId: chi.URLParam(r, "userID"), Role: body.Role}); err != nil {
		respond(w, nil, err, http.StatusNoContent)
		return
	}
	w.WriteHeader(204)
}
