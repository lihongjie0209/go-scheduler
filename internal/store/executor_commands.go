package store

import (
	"context"
	"fmt"
	"time"
)

const maxExecutorCommandErrorBytes = 4096

type ExecutorCommand struct {
	ID, TenantID, RunID, ExecutorAddress, Type, Reason string
	ExternalExecutionID, JobID, ScriptLanguage         string
	KubernetesClusterID                                string
	Attempts                                           int
}

type ExecutorCommandQueueStats struct {
	Pending          int64
	OldestPendingAge time.Duration
}

func (s *Store) ExecutorCommandQueueStats(ctx context.Context) (ExecutorCommandQueueStats, error) {
	var stats ExecutorCommandQueueStats
	var oldestPendingAgeSeconds float64
	err := s.pool.QueryRow(ctx, `SELECT count(*),COALESCE(GREATEST(EXTRACT(EPOCH FROM now()-min(created_at)),0),0) FROM executor_commands WHERE status='pending'`).Scan(&stats.Pending, &oldestPendingAgeSeconds)
	if err != nil {
		return ExecutorCommandQueueStats{}, fmt.Errorf("executor command queue stats: %w", err)
	}
	stats.OldestPendingAge = time.Duration(oldestPendingAgeSeconds * float64(time.Second))
	return stats, nil
}

func (s *Store) ClaimExecutorCommands(ctx context.Context, owner string, limit int) ([]ExecutorCommand, error) {
	if limit < 1 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `WITH picked AS (
		SELECT id FROM executor_commands
		WHERE status='pending' AND available_at<=now() AND (locked_until IS NULL OR locked_until<now())
		ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1
	), claimed AS (
		UPDATE executor_commands c SET locked_by=$2,locked_until=now()+interval '30 seconds',attempts=attempts+1
		FROM picked WHERE c.id=picked.id
		RETURNING c.id,c.tenant_id,c.run_id,c.executor_address,c.command_type,c.payload,c.attempts
	)
	SELECT id,tenant_id,run_id,executor_address,command_type,COALESCE(payload->>'reason',''),COALESCE(payload->>'external_execution_id',''),COALESCE(payload->>'job_id',''),COALESCE(payload->>'script_language',''),COALESCE(payload->>'kubernetes_cluster_id',''),attempts FROM claimed`, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim executor commands: %w", err)
	}
	defer rows.Close()
	commands := make([]ExecutorCommand, 0, limit)
	for rows.Next() {
		var command ExecutorCommand
		if err = rows.Scan(&command.ID, &command.TenantID, &command.RunID, &command.ExecutorAddress, &command.Type, &command.Reason, &command.ExternalExecutionID, &command.JobID, &command.ScriptLanguage, &command.KubernetesClusterID, &command.Attempts); err != nil {
			return nil, err
		}
		commands = append(commands, command)
	}
	return commands, rows.Err()
}

func (s *Store) CompleteExecutorCommand(ctx context.Context, owner, id string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE executor_commands SET status='delivered',delivered_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1 AND status='pending' AND locked_by=$2 AND locked_until>=now()`, id, owner)
	if err != nil {
		return fmt.Errorf("complete executor command: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) RetryExecutorCommand(ctx context.Context, owner, id, message string, delay time.Duration) error {
	if len(message) > maxExecutorCommandErrorBytes {
		message = message[:maxExecutorCommandErrorBytes]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE executor_commands SET available_at=now()+$3*interval '1 second',locked_by=NULL,locked_until=NULL,last_error=$4 WHERE id=$1 AND status='pending' AND locked_by=$2 AND locked_until>=now()`, id, owner, delay.Seconds(), message)
	if err != nil {
		return fmt.Errorf("retry executor command: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}
