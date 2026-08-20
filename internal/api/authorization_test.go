package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthorizationMiddleware(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		middleware func(http.Handler) http.Handler
		principal  principal
		want       int
	}{
		{name: "writer accepts developer", middleware: tenantWriterOnly, principal: principal{TenantID: "tenant", Role: "developer"}, want: http.StatusNoContent},
		{name: "writer rejects viewer", middleware: tenantWriterOnly, principal: principal{TenantID: "tenant", Role: "viewer"}, want: http.StatusForbidden},
		{name: "writer requires tenant", middleware: tenantWriterOnly, principal: principal{Role: "owner"}, want: http.StatusBadRequest},
		{name: "tenant admin accepts admin", middleware: tenantAdminOnly, principal: principal{TenantID: "tenant", Role: "admin"}, want: http.StatusNoContent},
		{name: "tenant admin rejects developer", middleware: tenantAdminOnly, principal: principal{TenantID: "tenant", Role: "developer"}, want: http.StatusForbidden},
		{name: "platform admin accepts platform administrator", middleware: platformAdminOnly, principal: principal{PlatformAdmin: true}, want: http.StatusNoContent},
		{name: "platform admin rejects tenant owner", middleware: platformAdminOnly, principal: principal{TenantID: "tenant", Role: "owner"}, want: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			handler := test.middleware(next)
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request = request.WithContext(context.WithValue(request.Context(), principalKey{}, test.principal))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestRoutesSeparatePublicAndAuthenticatedEndpoints(t *testing.T) {
	t.Parallel()
	handler := NewServer(nil, nil).Routes()
	for _, test := range []struct {
		name, path string
		want       int
	}{
		{name: "liveness is public", path: "/health/live", want: http.StatusOK},
		{name: "jobs require authentication", path: "/api/v1/jobs", want: http.StatusUnauthorized},
		{name: "platform routes require authentication", path: "/api/v1/platform/users", want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}
