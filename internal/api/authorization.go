package api

import (
	"context"
	"net/http"
	"strings"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

type principalKey struct{}

type principal struct {
	TenantID, UserID, Role string
	PlatformAdmin          bool
}

func canWriteTenant(p principal) bool {
	return p.TenantID != "" && (p.Role == "owner" || p.Role == "admin" || p.Role == "developer")
}

func canAdministerTenant(p principal) bool {
	return p.TenantID != "" && (p.Role == "owner" || p.Role == "admin")
}

func canAdministerPlatform(p principal) bool { return p.PlatformAdmin }

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || token == r.Header.Get("Authorization") {
			writeError(w, http.StatusUnauthorized, "missing API key")
			return
		}
		var p principal
		if strings.HasPrefix(token, "gsk_") {
			out, err := s.client.AuthenticateAPIKey(r.Context(), &schedulerv1.AuthenticateAPIKeyRequest{Token: token})
			if err != nil {
				writeError(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			p = principal{TenantID: out.GetTenantId(), Role: out.GetRole()}
		} else {
			claims, err := s.auth.Parse(token)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "invalid access token")
				return
			}
			user, err := s.client.GetUser(r.Context(), &schedulerv1.GetUserRequest{Id: claims.Subject})
			if err != nil || user.GetDisabled() {
				writeError(w, http.StatusUnauthorized, "account unavailable")
				return
			}
			p = principal{UserID: claims.Subject, PlatformAdmin: claims.PlatformAdmin}
			if tenant := r.Header.Get("X-Tenant-ID"); tenant != "" {
				role, roleErr := s.client.GetMembershipRole(r.Context(), &schedulerv1.GetMembershipRoleRequest{TenantId: tenant, UserId: p.UserID})
				if roleErr != nil {
					writeError(w, http.StatusForbidden, "tenant access denied")
					return
				}
				p.TenantID = tenant
				p.Role = role.GetRole()
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
		writeError(w, http.StatusBadRequest, "X-Tenant-ID is required")
		return false
	}
	if !canWriteTenant(p) {
		writeError(w, http.StatusForbidden, "write permission denied")
		return false
	}
	return true
}

func requireTenantAdmin(w http.ResponseWriter, r *http.Request) bool {
	p := getPrincipal(r.Context())
	if p.TenantID == "" {
		writeError(w, http.StatusBadRequest, "X-Tenant-ID is required")
		return false
	}
	if !canAdministerTenant(p) {
		writeError(w, http.StatusForbidden, "tenant admin permission required")
		return false
	}
	return true
}

func requirePlatformAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !canAdministerPlatform(getPrincipal(r.Context())) {
		writeError(w, http.StatusForbidden, "platform admin required")
		return false
	}
	return true
}

func tenantWriterOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := getPrincipal(r.Context())
		if p.TenantID == "" {
			writeError(w, http.StatusBadRequest, "X-Tenant-ID is required")
			return
		}
		if !canWriteTenant(p) {
			writeError(w, http.StatusForbidden, "write permission denied")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func tenantAdminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := getPrincipal(r.Context())
		if p.TenantID == "" {
			writeError(w, http.StatusBadRequest, "X-Tenant-ID is required")
			return
		}
		if !canAdministerTenant(p) {
			writeError(w, http.StatusForbidden, "tenant admin permission required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func platformAdminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !canAdministerPlatform(getPrincipal(r.Context())) {
			writeError(w, http.StatusForbidden, "platform admin required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
