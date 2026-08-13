package bootstrapcmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
)

func Run() {
	databaseURL := os.Getenv("DATABASE_URL")
	tenantName := os.Getenv("TENANT_NAME")
	adminEmail, adminPassword := os.Getenv("ADMIN_EMAIL"), os.Getenv("ADMIN_PASSWORD")
	if databaseURL == "" || tenantName == "" || adminEmail == "" || adminPassword == "" {
		fatal("DATABASE_URL, TENANT_NAME, ADMIN_EMAIL and ADMIN_PASSWORD are required")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		fatal(err.Error())
	}
	key := "gsk_" + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(key))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	defer func() { _ = conn.Close(ctx) }()
	var existingUserID, existingTenantID string
	err = conn.QueryRow(ctx, `SELECT u.id,tm.tenant_id FROM users u JOIN tenant_memberships tm ON tm.user_id=u.id WHERE lower(u.email)=lower($1) ORDER BY tm.created_at LIMIT 1`, adminEmail).Scan(&existingUserID, &existingTenantID)
	if err == nil {
		fmt.Printf("already_initialized=true\ntenant_id=%s\nadmin_user_id=%s\n", existingTenantID, existingUserID)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		fatal(err.Error())
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		fatal(err.Error())
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenantID string
	if err = tx.QueryRow(ctx, `INSERT INTO tenants(name) VALUES($1) RETURNING id`, tenantName).Scan(&tenantID); err != nil {
		fatal(err.Error())
	}
	if _, err = tx.Exec(ctx, `INSERT INTO api_keys(tenant_id,name,key_hash,role) VALUES($1,'bootstrap',$2,'owner')`, tenantID, hash[:]); err != nil {
		fatal(err.Error())
	}
	passwordHash, err := auth.HashPassword(adminPassword)
	if err != nil {
		fatal(err.Error())
	}
	var userID string
	if err = tx.QueryRow(ctx, `INSERT INTO users(email,password_hash,platform_admin) VALUES(lower($1),$2,true) RETURNING id`, adminEmail, passwordHash).Scan(&userID); err != nil {
		fatal(err.Error())
	}
	if _, err = tx.Exec(ctx, `INSERT INTO tenant_memberships(tenant_id,user_id,role) VALUES($1,$2,'owner')`, tenantID, userID); err != nil {
		fatal(err.Error())
	}
	if err = tx.Commit(ctx); err != nil {
		fatal(err.Error())
	}
	fmt.Printf("tenant_id=%s\nadmin_user_id=%s\napi_key=%s\n", tenantID, userID, key)
}
func fatal(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
