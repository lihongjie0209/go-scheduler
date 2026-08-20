package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(s.recoverer, s.requestLog)
	s.registerPublicRoutes(r)
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(s.authenticate)
		s.registerAuthenticatedRoutes(r)
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

func (s *Server) registerPublicRoutes(r chi.Router) {
	r.Get("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/health/ready", s.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Post("/api/v1/callbacks/{runID}", s.completeCallback)
	r.Post("/api/v1/runs/{runID}/logs", s.appendRunLogs)
	r.Post("/api/v1/auth/login", s.login)
	r.Post("/api/v1/auth/refresh", s.refresh)
	r.Post("/api/v1/auth/logout", s.logout)
}

func (s *Server) registerAuthenticatedRoutes(r chi.Router) {
	r.Get("/auth/me", s.me)
	r.Get("/dashboard", s.dashboard)
	r.Get("/reports/runs", s.runReport)
	r.Post("/schedules/preview", s.previewSchedule)
	s.registerTenantReadRoutes(r)

	r.Group(func(r chi.Router) {
		r.Use(tenantWriterOnly)
		s.registerTenantWriteRoutes(r)
	})
	r.Group(func(r chi.Router) {
		r.Use(tenantAdminOnly)
		s.registerTenantAdminRoutes(r)
	})
	r.Group(func(r chi.Router) {
		r.Use(platformAdminOnly)
		s.registerPlatformAdminRoutes(r)
	})
}

func (s *Server) registerTenantReadRoutes(r chi.Router) {
	r.Get("/jobs", s.listJobs)
	r.Get("/jobs/{id}", s.getJob)
	r.Get("/jobs/{id}/script-versions", s.listJobScriptVersions)
	r.Get("/jobs/{id}/dependencies", s.getJobDependencies)
	r.Get("/runs", s.listRuns)
	r.Get("/runs/{id}", s.getRun)
	r.Get("/runs/{id}/logs", s.listRunLogs)
	r.Get("/executor-groups", s.listExecutorGroups)
	r.Get("/executor-groups/{id}/nodes", s.listExecutorNodes)
	r.Get("/kubernetes-clusters", s.listKubernetesClusters)
	r.Get("/kubernetes-clusters/{id}", s.getKubernetesCluster)
	r.Get("/notification-channels", s.listNotificationChannels)
	r.Get("/notification-history", s.listNotificationHistory)
}

func (s *Server) registerTenantWriteRoutes(r chi.Router) {
	r.Post("/jobs", s.createJob)
	r.Put("/jobs/{id}", s.updateJob)
	r.Post("/jobs/{id}/script-versions/{versionID}/rollback", s.rollbackJobScriptVersion)
	r.Post("/jobs/{id}/start", s.startJob)
	r.Post("/jobs/{id}/stop", s.stopJob)
	r.Delete("/jobs/{id}", s.deleteJob)
	r.Post("/jobs/{id}/trigger", s.triggerJob)
	r.Put("/jobs/{id}/dependencies", s.setJobDependencies)
	r.Post("/runs/purge", s.purgeRunHistory)
	r.Post("/runs/{id}/cancel", s.cancelRun)
	r.Post("/executor-groups", s.createExecutorGroup)
	r.Put("/executor-groups/{id}", s.updateExecutorGroup)
	r.Delete("/executor-groups/{id}", s.deleteExecutorGroup)
	r.Put("/executor-groups/{id}/nodes/{nodeID}", s.registerExecutorNode)
	r.Delete("/executor-groups/{id}/nodes/{nodeID}", s.unregisterExecutorNode)
	r.Post("/notification-channels", s.createNotificationChannel)
	r.Put("/notification-channels/{id}", s.updateNotificationChannel)
	r.Put("/notification-channels/{id}/enabled", s.setNotificationChannelEnabled)
	r.Delete("/notification-channels/{id}", s.deleteNotificationChannel)
}

func (s *Server) registerTenantAdminRoutes(r chi.Router) {
	r.Post("/kubernetes-clusters", s.createKubernetesCluster)
	r.Put("/kubernetes-clusters/{id}", s.updateKubernetesCluster)
	r.Delete("/kubernetes-clusters/{id}", s.deleteKubernetesCluster)
	r.Get("/api-keys", s.listAPIKeys)
	r.Post("/api-keys", s.createAPIKey)
	r.Delete("/api-keys/{id}", s.revokeAPIKey)
}

func (s *Server) registerPlatformAdminRoutes(r chi.Router) {
	r.Post("/platform/users", s.createUser)
	r.Get("/platform/users", s.listUsers)
	r.Patch("/platform/users/{id}", s.patchUser)
	r.Post("/platform/tenants", s.createTenant)
	r.Get("/platform/tenants", s.listTenants)
	r.Get("/platform/instances", s.listInstances)
	r.Put("/platform/tenants/{tenantID}/members/{userID}", s.putMembership)
	r.Get("/platform/tenants/{tenantID}/members", s.listMembers)
	r.Delete("/platform/tenants/{tenantID}/members/{userID}", s.deleteMembership)
}
