package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type RunLogInput struct {
	EntryID, Stream, Content string
}

type RunLogEntry struct {
	Cursor                   int64
	EntryID, Stream, Content string
	CreatedAt                time.Time
}

func (s *Store) ActivateRunToken(ctx context.Context, runID string, tokenHash []byte, deadline time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs SET callback_token_hash=$2,callback_deadline=$3 WHERE id=$1 AND status='running'`, runID, tokenHash, deadline)
	if err != nil {
		return fmt.Errorf("activate run token: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) AppendRunLogs(ctx context.Context, runID string, tokenHash []byte, entries []RunLogInput) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenantID string
	var storedHash []byte
	err = tx.QueryRow(ctx, `SELECT tenant_id,callback_token_hash FROM job_runs WHERE id=$1 AND status IN ('running','waiting_callback') AND callback_deadline>now() FOR UPDATE`, runID).Scan(&tenantID, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if subtle.ConstantTimeCompare(storedHash, tokenHash) != 1 {
		return 0, ErrNotFound
	}
	var cursor int64
	for _, entry := range entries {
		var id int64
		err = tx.QueryRow(ctx, `INSERT INTO job_run_logs(tenant_id,run_id,entry_id,stream,content) VALUES($1,$2,$3,$4,$5) ON CONFLICT(run_id,entry_id) DO UPDATE SET entry_id=EXCLUDED.entry_id RETURNING id`, tenantID, runID, entry.EntryID, entry.Stream, entry.Content).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("append run log: %w", err)
		}
		if id > cursor {
			cursor = id
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return cursor, nil
}

func (s *Store) ListRunLogs(ctx context.Context, tenantID, runID string, after int64, limit int) ([]RunLogEntry, int64, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE tenant_id=$1 AND id=$2)`, tenantID, runID).Scan(&exists); err != nil {
		return nil, after, err
	}
	if !exists {
		return nil, after, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT id,entry_id,stream,content,created_at FROM job_run_logs WHERE tenant_id=$1 AND run_id=$2 AND id>$3 ORDER BY id LIMIT $4`, tenantID, runID, after, limit)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	logs := make([]RunLogEntry, 0, limit)
	next := after
	for rows.Next() {
		var entry RunLogEntry
		if err = rows.Scan(&entry.Cursor, &entry.EntryID, &entry.Stream, &entry.Content, &entry.CreatedAt); err != nil {
			return nil, after, err
		}
		logs = append(logs, entry)
		next = entry.Cursor
	}
	return logs, next, rows.Err()
}
