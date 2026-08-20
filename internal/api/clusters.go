package api

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kubernetesClusterRequest struct {
	Name                  string `json:"name"`
	AuthMode              string `json:"auth_mode"`
	APIServer             string `json:"api_server"`
	Namespace             string `json:"namespace"`
	Kubeconfig            string `json:"kubeconfig"`
	Token                 string `json:"token"`
	CAData                string `json:"ca_data"`
	InsecureSkipTLSVerify bool   `json:"insecure_skip_tls_verify"`
	MaxConcurrentJobs     int32  `json:"max_concurrent_jobs"`
	Version               int64  `json:"version"`
}

func publicKubernetesCluster(cluster *schedulerv1.KubernetesCluster) map[string]any {
	return map[string]any{"id": cluster.GetId(), "tenant_id": cluster.GetTenantId(), "name": cluster.GetName(), "auth_mode": cluster.GetAuthMode(), "api_server": cluster.GetApiServer(), "namespace": cluster.GetNamespace(), "insecure_skip_tls_verify": cluster.GetInsecureSkipTlsVerify(), "max_concurrent_jobs": cluster.GetMaxConcurrentJobs(), "credentials_configured": true, "version": cluster.GetVersion(), "created_at": cluster.GetCreatedAt().AsTime(), "updated_at": cluster.GetUpdatedAt().AsTime()}
}

func clusterFromRequest(tenant, id string, body kubernetesClusterRequest) *schedulerv1.KubernetesCluster {
	return &schedulerv1.KubernetesCluster{
		Id: id, TenantId: tenant, Name: body.Name, AuthMode: body.AuthMode,
		ApiServer: body.APIServer, Namespace: body.Namespace,
		Kubeconfig: strings.TrimSpace(body.Kubeconfig), Token: strings.TrimSpace(body.Token), CaData: strings.TrimSpace(body.CAData),
		InsecureSkipTlsVerify: body.InsecureSkipTLSVerify, MaxConcurrentJobs: body.MaxConcurrentJobs, Version: body.Version,
	}
}

func (s *Server) listKubernetesClusters(w http.ResponseWriter, r *http.Request) {
	if tenantID(r.Context()) == "" {
		writeError(w, 400, "X-Tenant-ID is required")
		return
	}
	resp, err := s.client.ListKubernetesClusters(r.Context(), &schedulerv1.ListKubernetesClustersRequest{TenantId: tenantID(r.Context())})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	out := make([]map[string]any, 0, len(resp.GetClusters()))
	for _, cluster := range resp.GetClusters() {
		out = append(out, publicKubernetesCluster(cluster))
	}
	writeJSON(w, 200, map[string]any{"clusters": out})
}

func (s *Server) getKubernetesCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := s.client.GetKubernetesCluster(r.Context(), &schedulerv1.GetKubernetesClusterRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id")})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			writeError(w, 404, "kubernetes cluster not found")
			return
		}
		respond(w, nil, err, http.StatusOK)
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
	cluster, err := s.client.CreateKubernetesCluster(r.Context(), &schedulerv1.CreateKubernetesClusterRequest{Cluster: clusterFromRequest(tenantID(r.Context()), "", body)})
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			writeError(w, 400, status.Convert(err).Message())
			return
		}
		respond(w, nil, err, http.StatusCreated)
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
	cluster, err := s.client.UpdateKubernetesCluster(r.Context(), &schedulerv1.UpdateKubernetesClusterRequest{Cluster: clusterFromRequest(tenantID(r.Context()), chi.URLParam(r, "id"), body)})
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			writeError(w, 404, "kubernetes cluster not found")
		case codes.Aborted:
			writeError(w, 409, "kubernetes cluster version conflict")
		case codes.InvalidArgument:
			writeError(w, 400, status.Convert(err).Message())
		default:
			respond(w, nil, err, http.StatusOK)
		}
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
	_, err = s.client.DeleteKubernetesCluster(r.Context(), &schedulerv1.DeleteKubernetesClusterRequest{TenantId: tenantID(r.Context()), Id: chi.URLParam(r, "id"), Version: version})
	if err != nil {
		switch status.Code(err) {
		case codes.FailedPrecondition:
			writeError(w, 409, "kubernetes cluster is referenced by a job")
		case codes.Aborted, codes.NotFound:
			writeError(w, 409, "kubernetes cluster version conflict")
		default:
			respond(w, nil, err, http.StatusNoContent)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
