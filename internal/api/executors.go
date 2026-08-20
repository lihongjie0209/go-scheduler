package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

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
