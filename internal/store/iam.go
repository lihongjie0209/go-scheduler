package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type User struct {
	ID, Email, PasswordHash string
	PlatformAdmin, Disabled bool
}

type TenantAccess struct {
	ID, Name, Role    string
	MaxConcurrentRuns int
}

type RefreshSession struct {
	ID, FamilyID, UserID string
	ExpiresAt            time.Time
}

func (s *Store) GetUserByEmail(ctx context.Context, email string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx, `SELECT id,email,password_hash,platform_admin,disabled FROM users WHERE lower(email)=lower($1)`, email).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.PlatformAdmin, &u.Disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("get user: %w", err)
	}
	return u, nil
}

func (s *Store) GetUser(ctx context.Context, id string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx, `SELECT id,email,password_hash,platform_admin,disabled FROM users WHERE id=$1`, id).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.PlatformAdmin, &u.Disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("get user: %w", err)
	}
	return u, nil
}

func (s *Store) UserTenants(ctx context.Context, userID string, platformAdmin bool) ([]TenantAccess, error) {
	query := `SELECT t.id,t.name,tm.role,t.max_concurrent_runs FROM tenant_memberships tm JOIN tenants t ON t.id=tm.tenant_id WHERE tm.user_id=$1 ORDER BY t.name`
	args := []any{userID}
	if platformAdmin {
		query = `SELECT id,name,'owner',max_concurrent_runs FROM tenants ORDER BY name`
		args = nil
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list user tenants: %w", err)
	}
	defer rows.Close()
	out := make([]TenantAccess, 0)
	for rows.Next() {
		var x TenantAccess
		if err = rows.Scan(&x.ID, &x.Name, &x.Role, &x.MaxConcurrentRuns); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func randomToken() (string, []byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate refresh token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	return token, hash[:], nil
}

func (s *Store) CreateRefreshSession(ctx context.Context, userID string, ttl time.Duration) (string, RefreshSession, error) {
	token, hash, err := randomToken()
	if err != nil {
		return "", RefreshSession{}, err
	}
	session := RefreshSession{ID: uuid.NewString(), FamilyID: uuid.NewString(), UserID: userID, ExpiresAt: time.Now().Add(ttl)}
	_, err = s.pool.Exec(ctx, `INSERT INTO refresh_sessions(id,family_id,user_id,token_hash,expires_at) VALUES($1,$2,$3,$4,$5)`, session.ID, session.FamilyID, userID, hash, session.ExpiresAt)
	if err != nil {
		return "", RefreshSession{}, fmt.Errorf("create refresh session: %w", err)
	}
	return token, session, nil
}

func (s *Store) RotateRefreshSession(ctx context.Context, raw string, ttl time.Duration) (string, RefreshSession, error) {
	hash := sha256.Sum256([]byte(raw))
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", RefreshSession{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var old RefreshSession
	var consumed, revoked *time.Time
	err = tx.QueryRow(ctx, `SELECT id,family_id,user_id,expires_at,consumed_at,revoked_at FROM refresh_sessions WHERE token_hash=$1 FOR UPDATE`, hash[:]).Scan(&old.ID, &old.FamilyID, &old.UserID, &old.ExpiresAt, &consumed, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", RefreshSession{}, ErrNotFound
	}
	if err != nil {
		return "", RefreshSession{}, fmt.Errorf("get refresh session: %w", err)
	}
	if consumed != nil || revoked != nil || time.Now().After(old.ExpiresAt) {
		if _, err = tx.Exec(ctx, `UPDATE refresh_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE family_id=$1`, old.FamilyID); err != nil {
			return "", RefreshSession{}, fmt.Errorf("revoke replayed refresh session family: %w", err)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return "", RefreshSession{}, commitErr
		}
		return "", RefreshSession{}, ErrConflict
	}
	token, newHash, err := randomToken()
	if err != nil {
		return "", RefreshSession{}, err
	}
	next := RefreshSession{ID: uuid.NewString(), FamilyID: old.FamilyID, UserID: old.UserID, ExpiresAt: time.Now().Add(ttl)}
	if _, err = tx.Exec(ctx, `INSERT INTO refresh_sessions(id,family_id,user_id,token_hash,expires_at) VALUES($1,$2,$3,$4,$5)`, next.ID, next.FamilyID, next.UserID, newHash, next.ExpiresAt); err != nil {
		return "", RefreshSession{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE refresh_sessions SET consumed_at=now(),replaced_by=$2 WHERE id=$1`, old.ID, next.ID); err != nil {
		return "", RefreshSession{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", RefreshSession{}, err
	}
	return token, next, nil
}

func (s *Store) RevokeRefreshSession(ctx context.Context, raw string) error {
	hash := sha256.Sum256([]byte(raw))
	_, err := s.pool.Exec(ctx, `UPDATE refresh_sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE family_id=(SELECT family_id FROM refresh_sessions WHERE token_hash=$1)`, hash[:])
	return err
}
func (s *Store) CreateUser(ctx context.Context, email, passwordHash string, platformAdmin bool) (User, error) {
	u := User{ID: uuid.NewString(), Email: email, PasswordHash: passwordHash, PlatformAdmin: platformAdmin}
	err := s.pool.QueryRow(ctx, `INSERT INTO users(id,email,password_hash,platform_admin) VALUES($1,lower($2),$3,$4) RETURNING email`, u.ID, email, passwordHash, platformAdmin).Scan(&u.Email)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}
func (s *Store) CreateTenant(ctx context.Context, name string) (string, error) {
	id := uuid.NewString()
	_, err := s.pool.Exec(ctx, `INSERT INTO tenants(id,name) VALUES($1,$2)`, id, name)
	if err != nil {
		return "", fmt.Errorf("create tenant: %w", err)
	}
	return id, nil
}
func (s *Store) AddMembership(ctx context.Context, tenantID, userID, role string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tenant_memberships(tenant_id,user_id,role) VALUES($1,$2,$3) ON CONFLICT(tenant_id,user_id) DO UPDATE SET role=EXCLUDED.role`, tenantID, userID, role)
	if err != nil {
		return fmt.Errorf("add membership: %w", err)
	}
	return nil
}
func (s *Store) GetMembershipRole(ctx context.Context, tenantID, userID string) (string, error) {
	var role string
	err := s.pool.QueryRow(ctx, `SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get membership: %w", err)
	}
	return role, nil
}
