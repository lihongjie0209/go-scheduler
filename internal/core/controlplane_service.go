package core

import (
	"context"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type HealthChecker interface {
	Ping(context.Context) error
}

type UserReader interface {
	GetUser(context.Context, string) (store.User, error)
	GetUserByEmail(context.Context, string) (store.User, error)
	ListUsers(context.Context) ([]store.UserSummary, error)
}

type UserWriter interface {
	CreateUser(context.Context, string, string, bool) (store.User, error)
	SetUserDisabled(context.Context, string, bool) error
}

type AccessReader interface {
	GetMembershipRole(context.Context, string, string) (string, error)
	UserTenants(context.Context, string, bool) ([]store.TenantAccess, error)
}

type RefreshSessionRepository interface {
	CreateRefreshSession(context.Context, string, time.Duration) (string, store.RefreshSession, error)
	RotateRefreshSession(context.Context, string, time.Duration) (string, store.RefreshSession, error)
	RevokeRefreshSession(context.Context, string) error
}

type APIKeyAuthenticator interface {
	AuthenticateAPIKey(context.Context, string) (string, string, error)
}

type IdentityService struct {
	health   HealthChecker
	users    UserReader
	writes   UserWriter
	access   AccessReader
	sessions RefreshSessionRepository
	apiKeys  APIKeyAuthenticator
}

func NewIdentityService(health HealthChecker, users UserReader, writes UserWriter, access AccessReader, sessions RefreshSessionRepository, apiKeys APIKeyAuthenticator) *IdentityService {
	return &IdentityService{health: health, users: users, writes: writes, access: access, sessions: sessions, apiKeys: apiKeys}
}

func (s *IdentityService) Ping(ctx context.Context) error { return s.health.Ping(ctx) }
func (s *IdentityService) AuthenticateAPIKey(ctx context.Context, token string) (string, string, error) {
	return s.apiKeys.AuthenticateAPIKey(ctx, token)
}
func (s *IdentityService) GetUser(ctx context.Context, id string) (store.User, error) {
	return s.users.GetUser(ctx, id)
}
func (s *IdentityService) GetUserByEmail(ctx context.Context, email string) (store.User, error) {
	return s.users.GetUserByEmail(ctx, email)
}
func (s *IdentityService) GetMembershipRole(ctx context.Context, tenantID, userID string) (string, error) {
	return s.access.GetMembershipRole(ctx, tenantID, userID)
}
func (s *IdentityService) UserTenants(ctx context.Context, userID string, platformAdmin bool) ([]store.TenantAccess, error) {
	return s.access.UserTenants(ctx, userID, platformAdmin)
}
func (s *IdentityService) CreateRefreshSession(ctx context.Context, userID string, ttl time.Duration) (string, store.RefreshSession, error) {
	return s.sessions.CreateRefreshSession(ctx, userID, ttl)
}
func (s *IdentityService) RotateRefreshSession(ctx context.Context, token string, ttl time.Duration) (string, store.RefreshSession, error) {
	return s.sessions.RotateRefreshSession(ctx, token, ttl)
}
func (s *IdentityService) RevokeRefreshSession(ctx context.Context, token string) error {
	return s.sessions.RevokeRefreshSession(ctx, token)
}
func (s *IdentityService) ListUsers(ctx context.Context) ([]store.UserSummary, error) {
	return s.users.ListUsers(ctx)
}
func (s *IdentityService) CreateUser(ctx context.Context, email, passwordHash string, platformAdmin bool) (store.User, error) {
	return s.writes.CreateUser(ctx, email, passwordHash, platformAdmin)
}
func (s *IdentityService) SetUserDisabled(ctx context.Context, id string, disabled bool) error {
	return s.writes.SetUserDisabled(ctx, id, disabled)
}

type TenantReader interface {
	ListTenants(context.Context) ([]store.TenantSummary, error)
	TenantMembers(context.Context, string) ([]store.MemberSummary, error)
	Dashboard(context.Context, string) (store.Dashboard, error)
}

type TenantWriter interface {
	CreateTenant(context.Context, string) (string, error)
	AddMembership(context.Context, string, string, string) error
	DeleteMembership(context.Context, string, string) error
}

type APIKeyRepository interface {
	ListAPIKeys(context.Context, string) ([]store.APIKey, error)
	CreateAPIKey(context.Context, string, string, string) (store.APIKey, string, error)
	RevokeAPIKey(context.Context, string, string) error
}

type TenancyService struct {
	reader  TenantReader
	writer  TenantWriter
	apiKeys APIKeyRepository
}

func NewTenancyService(reader TenantReader, writer TenantWriter, apiKeys APIKeyRepository) *TenancyService {
	return &TenancyService{reader: reader, writer: writer, apiKeys: apiKeys}
}

func (s *TenancyService) List(ctx context.Context) ([]store.TenantSummary, error) {
	return s.reader.ListTenants(ctx)
}
func (s *TenancyService) Create(ctx context.Context, name string) (string, error) {
	return s.writer.CreateTenant(ctx, name)
}
func (s *TenancyService) Members(ctx context.Context, tenantID string) ([]store.MemberSummary, error) {
	return s.reader.TenantMembers(ctx, tenantID)
}
func (s *TenancyService) AddMembership(ctx context.Context, tenantID, userID, role string) error {
	return s.writer.AddMembership(ctx, tenantID, userID, role)
}
func (s *TenancyService) DeleteMembership(ctx context.Context, tenantID, userID string) error {
	return s.writer.DeleteMembership(ctx, tenantID, userID)
}
func (s *TenancyService) ListAPIKeys(ctx context.Context, tenantID string) ([]store.APIKey, error) {
	return s.apiKeys.ListAPIKeys(ctx, tenantID)
}
func (s *TenancyService) CreateAPIKey(ctx context.Context, tenantID, name, role string) (store.APIKey, string, error) {
	return s.apiKeys.CreateAPIKey(ctx, tenantID, name, role)
}
func (s *TenancyService) RevokeAPIKey(ctx context.Context, tenantID, id string) error {
	return s.apiKeys.RevokeAPIKey(ctx, tenantID, id)
}
func (s *TenancyService) Dashboard(ctx context.Context, tenantID string) (store.Dashboard, error) {
	return s.reader.Dashboard(ctx, tenantID)
}
