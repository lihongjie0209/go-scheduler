package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tenantAccess struct {
	ID, Name, Role    string
	MaxConcurrentRuns int
}

// dummyPasswordHash is public, deliberately non-matching Argon2 work used to
// equalize unknown-account login timing; it is not an authentication secret.
const dummyPasswordHash = "$argon2id$v=19$m=65536,t=3,p=2$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" //nolint:gosec

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
	user, err := s.client.GetUserByEmail(r.Context(), &schedulerv1.GetUserByEmailRequest{Email: body.Email})
	passwordHash := user.GetPasswordHash()
	if err != nil {
		passwordHash = dummyPasswordHash
	}
	passwordValid := auth.VerifyPassword(passwordHash, body.Password)
	if err != nil || user.GetDisabled() || !passwordValid {
		writeError(w, 401, "invalid credentials")
		return
	}
	s.logins.reset(r.RemoteAddr, body.Email)
	token, err := s.auth.Issue(user.GetId(), user.GetPlatformAdmin())
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	refresh, err := s.client.CreateRefreshSession(r.Context(), &schedulerv1.CreateRefreshSessionRequest{UserId: user.GetId(), TtlSeconds: int64((7 * 24 * time.Hour) / time.Second)})
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	s.setRefreshCookie(w, refresh.GetToken(), 7*24*time.Hour)
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
	rotated, err := s.client.RotateRefreshSession(r.Context(), &schedulerv1.RotateRefreshSessionRequest{Token: cookie.Value, TtlSeconds: int64((7 * 24 * time.Hour) / time.Second)})
	if err != nil {
		s.clearRefreshCookie(w)
		writeError(w, 401, "refresh session expired")
		return
	}
	user, err := s.client.GetUser(r.Context(), &schedulerv1.GetUserRequest{Id: rotated.GetUserId()})
	if err != nil || user.GetDisabled() {
		s.clearRefreshCookie(w)
		writeError(w, 401, "account unavailable")
		return
	}
	token, err := s.auth.Issue(user.GetId(), user.GetPlatformAdmin())
	if err != nil {
		writeError(w, 500, "internal error")
		return
	}
	s.setRefreshCookie(w, rotated.GetToken(), 7*24*time.Hour)
	writeJSON(w, 200, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 900})
}
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("scheduler_refresh"); err == nil {
		_, _ = s.client.RevokeRefreshSession(r.Context(), &schedulerv1.RevokeRefreshSessionRequest{Token: c.Value})
	}
	s.clearRefreshCookie(w)
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	p := getPrincipal(r.Context())
	u, err := s.client.GetUser(r.Context(), &schedulerv1.GetUserRequest{Id: p.UserID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			writeError(w, 404, "user not found")
			return
		}
		respond(w, nil, err, http.StatusOK)
		return
	}
	resp, err := s.client.ListUserTenants(r.Context(), &schedulerv1.ListUserTenantsRequest{UserId: u.GetId(), PlatformAdmin: u.GetPlatformAdmin()})
	if err != nil {
		respond(w, nil, err, http.StatusOK)
		return
	}
	tenants := make([]tenantAccess, 0, len(resp.GetTenants()))
	for _, tenant := range resp.GetTenants() {
		tenants = append(tenants, tenantAccess{ID: tenant.GetId(), Name: tenant.GetName(), Role: tenant.GetRole(), MaxConcurrentRuns: int(tenant.GetMaxConcurrentRuns())})
	}
	writeJSON(w, 200, map[string]any{"id": u.GetId(), "email": u.GetEmail(), "platform_admin": u.GetPlatformAdmin(), "tenants": tenants})
}
