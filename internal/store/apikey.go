package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type APIKey struct {
	ID, TenantID, Name, Role string
	CreatedAt                time.Time
	RevokedAt                *time.Time
}

func (s *Store) CreateAPIKey(ctx context.Context, tenantID, name, role string) (APIKey, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return APIKey{}, "", fmt.Errorf("generate API key: %w", err)
	}
	token := "gsk_" + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	key := APIKey{ID: uuid.NewString(), TenantID: tenantID, Name: name, Role: role}
	err := s.pool.QueryRow(ctx, `INSERT INTO api_keys(id,tenant_id,name,key_hash,role) VALUES($1,$2,$3,$4,$5) RETURNING created_at`, key.ID, tenantID, name, hash[:], role).Scan(&key.CreatedAt)
	if err != nil {
		return APIKey{}, "", fmt.Errorf("create API key: %w", err)
	}
	return key, token, nil
}
func (s *Store) ListAPIKeys(ctx context.Context, tenantID string) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,tenant_id,name,role,created_at,revoked_at FROM api_keys WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var key APIKey
		if err = rows.Scan(&key.ID, &key.TenantID, &key.Name, &key.Role, &key.CreatedAt, &key.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}
func (s *Store) RevokeAPIKey(ctx context.Context, tenantID, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE tenant_id=$1 AND id=$2 AND revoked_at IS NULL`, tenantID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}
