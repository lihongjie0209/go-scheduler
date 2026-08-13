package store

import (
	"context"
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

func (s *Store) ActivateClaimedRunToken(ctx context.Context, run Run, tokenHash []byte, deadline time.Time) error {
	if err := requireRunLease(run); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs SET callback_token_hash=$2,callback_deadline=$3 WHERE id=$1 AND status='running' AND lease_token=$4`, run.ID, tokenHash, deadline, run.LeaseToken)
	if err != nil {
		return fmt.Errorf("activate run token: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) AppendRunLogs(ctx context.Context, runID string, tokenHash []byte, entries []RunLogInput) (int64, error) {
	if len(entries) == 1 {
		entry := entries[0]
		var cursor int64
		err := s.pool.QueryRow(ctx, `INSERT INTO job_run_logs(tenant_id,run_id,entry_id,stream,content)
			SELECT tenant_id,id,$3,$4,$5 FROM job_runs
			WHERE id=$1 AND status IN ('running','waiting_callback') AND callback_deadline>now() AND callback_token_hash=$2
			ON CONFLICT(run_id,entry_id) DO UPDATE SET entry_id=EXCLUDED.entry_id RETURNING id`, runID, tokenHash, entry.EntryID, entry.Stream, entry.Content).Scan(&cursor)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, ErrNotFound
			}
			return 0, fmt.Errorf("append run log: %w", err)
		}
		return cursor, nil
	}
	entryIDs := make([]string, len(entries))
	streams := make([]string, len(entries))
	contents := make([]string, len(entries))
	for index, entry := range entries {
		entryIDs[index], streams[index], contents[index] = entry.EntryID, entry.Stream, entry.Content
	}
	var cursor, inserted int64
	err := s.pool.QueryRow(ctx, `WITH valid_run AS (
	 SELECT tenant_id,id FROM job_runs WHERE id=$1 AND status IN ('running','waiting_callback') AND callback_deadline>now() AND callback_token_hash=$2
	), entries AS (
	 SELECT * FROM unnest($3::text[],$4::text[],$5::text[]) AS e(entry_id,stream,content)
	), inserted AS (
	 INSERT INTO job_run_logs(tenant_id,run_id,entry_id,stream,content)
	 SELECT r.tenant_id,r.id,e.entry_id,e.stream,e.content FROM valid_run r CROSS JOIN entries e
	 ON CONFLICT(run_id,entry_id) DO UPDATE SET entry_id=EXCLUDED.entry_id RETURNING id
	) SELECT COALESCE(max(id),0),count(*) FROM inserted`, runID, tokenHash, entryIDs, streams, contents).Scan(&cursor, &inserted)
	if err != nil {
		return 0, fmt.Errorf("append run logs: %w", err)
	}
	if inserted == 0 {
		return 0, ErrNotFound
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
