package store

import (
	"context"
	"fmt"
	"time"
)

const historyCleanupBatchSize = 10000

func (s *Store) CleanupAuxiliaryHistory(ctx context.Context, retention time.Duration) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin history cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_run_idempotency
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_run_idempotency AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean idempotency history: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_run_logs
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_run_logs AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean run logs: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM outbox_events
		WHERE published_at IS NOT NULL AND published_at<now()-$1*interval '1 second'
		ORDER BY published_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM outbox_events AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean outbox history: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_dependency_dispatches
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_dependency_dispatches AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean dependency dispatch history: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM executor_commands
		WHERE delivered_at IS NOT NULL AND delivered_at<now()-$1*interval '1 second'
		ORDER BY delivered_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM executor_commands AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean executor command history: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit history cleanup: %w", err)
	}
	return nil
}

func (s *Store) CleanupRunHistory(ctx context.Context, retention time.Duration) error {
	rows, err := s.pool.Query(ctx, `SELECT id FROM tenants ORDER BY id`)
	if err != nil {
		return fmt.Errorf("list tenants for run cleanup: %w", err)
	}
	tenantIDs := make([]string, 0)
	for rows.Next() {
		var tenantID string
		if err = rows.Scan(&tenantID); err != nil {
			rows.Close()
			return err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	before := time.Now().Add(-retention)
	for _, tenantID := range tenantIDs {
		if _, err = s.PurgeRunHistory(ctx, tenantID, "", before, 10000); err != nil {
			return fmt.Errorf("clean terminal runs for tenant %s: %w", tenantID, err)
		}
	}
	return nil
}
func (s *Store) PurgeRunHistory(ctx context.Context, tenantID, jobID string, before time.Time, limit int) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin run history purge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT id FROM job_runs WHERE tenant_id=$1 AND ($2='' OR job_id=$2::uuid) AND scheduled_at<$3 AND status IN ('succeeded','failed','timed_out','cancelled','skipped') ORDER BY scheduled_at LIMIT $4 FOR UPDATE SKIP LOCKED`, tenantID, jobID, before, limit)
	if err != nil {
		return 0, fmt.Errorf("select run history: %w", err)
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_run_logs WHERE run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete run logs: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_run_idempotency WHERE run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete run idempotency: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_dependency_dispatches WHERE parent_run_id=ANY($1::uuid[]) OR child_run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete dependency dispatches: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM job_runs WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, tenantID, ids)
	if err != nil {
		return 0, fmt.Errorf("delete run history: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit run history purge: %w", err)
	}
	return tag.RowsAffected(), nil
}
