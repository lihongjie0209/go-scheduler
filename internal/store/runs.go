package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Run struct {
	ID, TenantID, JobID, TriggerType, Status, RuntimeInput, ParentRunID, RetryOfRunID string
	ExternalExecutionID                                                               string
	ExecutorNodeID, ExecutorAddress                                                   string
	LeaseToken                                                                        string
	BroadcastGroupID                                                                  string
	ShardIndex, ShardTotal                                                            int32
	RescheduleOnTerminal                                                              bool
	OverrideAddresses                                                                 []string
	Attempt                                                                           int32
	ScheduledAt                                                                       time.Time
	StartedAt, FinishedAt                                                             *time.Time
	ResponseStatus                                                                    int32
	ErrorMessage                                                                      string
}

func requireRunLease(run Run) error {
	if run.ID == "" || run.LeaseToken == "" {
		return ErrConflict
	}
	return nil
}

func (s *Store) FailRun(ctx context.Context, r Run, state string, status int, errorMessage string, retryDelay *time.Duration) (*Run, error) {
	if state != "failed" && state != "timed_out" {
		return nil, fmt.Errorf("invalid failure state %q", state)
	}
	if err := requireRunLease(r); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var finishedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE job_runs SET status=$2,response_status=$3,error_message=$4,finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='running' AND lease_token=$5 RETURNING finished_at`, r.ID, state, status, errorMessage, r.LeaseToken).Scan(&finishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, r.ID, state); err != nil {
		return nil, err
	}
	if retryDelay == nil {
		if err = emitRunEventTx(ctx, tx, r.ID, state, "exhausted"); err != nil {
			return nil, err
		}
	}
	var retry *Run
	if retryDelay != nil {
		next, retryErr := insertRetryRunTx(ctx, tx, r, *retryDelay)
		err = retryErr
		if err != nil {
			return nil, err
		}
		retry = &next
	}
	if retryDelay == nil {
		if err = rearmFixedDelay(ctx, tx, r, finishedAt); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return retry, nil
}

func (s *Store) IsRunRunning(ctx context.Context, runID string) (bool, error) {
	var running bool
	if err := s.pool.QueryRow(ctx, `SELECT status='running' FROM job_runs WHERE id=$1`, runID).Scan(&running); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("read run state: %w", err)
	}
	return running, nil
}

func (s *Store) CancelRun(ctx context.Context, tenantID, runID, reason string) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := scanRun(tx.QueryRow(ctx, `UPDATE job_runs SET status='cancelled',finished_at=COALESCE(finished_at,now()),error_message=$3,lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE tenant_id=$1 AND id=$2 AND status IN ('pending','running','waiting_callback') RETURNING `+runSelectColumns, tenantID, runID, reason))
	if err == nil {
		if err = emitRunLifecycleEventTx(ctx, tx, run.ID, "cancelled"); err != nil {
			return Run{}, err
		}
		if run.ExecutorAddress != "" {
			if err = enqueueExecutorCancellationTx(ctx, tx, run, reason); err != nil {
				return Run{}, fmt.Errorf("enqueue executor cancellation: %w", err)
			}
		}
		if run.FinishedAt != nil {
			if err = rearmFixedDelay(ctx, tx, run, *run.FinishedAt); err != nil {
				return Run{}, err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return Run{}, err
		}
		return run, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Run{}, fmt.Errorf("cancel run: %w", err)
	}
	run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND id=$2`, tenantID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("read run after cancel: %w", err)
	}
	if run.Status == "cancelled" {
		if run.ExecutorAddress != "" {
			if err = enqueueExecutorCancellationTx(ctx, tx, run, run.ErrorMessage); err != nil {
				return Run{}, fmt.Errorf("ensure executor cancellation: %w", err)
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return Run{}, err
		}
		return run, nil
	}
	return Run{}, ErrNotCancellable
}

func enqueueExecutorCancellationTx(ctx context.Context, tx pgx.Tx, run Run, reason string) error {
	var scriptLanguage, kubernetesClusterID string
	if err := tx.QueryRow(ctx, `SELECT script_language,COALESCE(kubernetes_cluster_id::text,'') FROM jobs WHERE tenant_id=$1 AND id=$2`, run.TenantID, run.JobID).Scan(&scriptLanguage, &kubernetesClusterID); err != nil {
		return fmt.Errorf("read cancellation execution target: %w", err)
	}
	_, err := tx.Exec(ctx, `INSERT INTO executor_commands(tenant_id,run_id,executor_address,command_type,payload)
		VALUES($1,$2,$3,'cancel',jsonb_build_object('reason',$4::text,'external_execution_id',$5::text,'job_id',$6::text,'script_language',$7::text,'kubernetes_cluster_id',$8::text))
		ON CONFLICT(tenant_id,run_id,command_type) DO UPDATE SET payload=executor_commands.payload||(EXCLUDED.payload-'reason')`, run.TenantID, run.ID, run.ExecutorAddress, reason, run.ExternalExecutionID, run.JobID, scriptLanguage, kubernetesClusterID)
	return err
}

const runSelectColumns = `id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,started_at,finished_at,COALESCE(response_status,0),COALESCE(error_message,''),runtime_input,COALESCE(parent_run_id::text,''),COALESCE(retry_of_run_id::text,''),external_execution_id,COALESCE(executor_node_id,''),COALESCE(executor_address,''),COALESCE(broadcast_group_id::text,''),COALESCE(shard_index,0),COALESCE(shard_total,0),reschedule_on_terminal,override_addresses`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.StartedAt, &r.FinishedAt, &r.ResponseStatus, &r.ErrorMessage, &r.RuntimeInput, &r.ParentRunID, &r.RetryOfRunID, &r.ExternalExecutionID, &r.ExecutorNodeID, &r.ExecutorAddress, &r.BroadcastGroupID, &r.ShardIndex, &r.ShardTotal, &r.RescheduleOnTerminal, &r.OverrideAddresses)
	return r, err
}

func (s *Store) GetRun(ctx context.Context, tenantID, runID string) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND id=$2`, tenantID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return r, err
}

func (s *Store) ListRuns(ctx context.Context, tenantID, jobID string, limit int) ([]Run, error) {
	return s.ListRunsFiltered(ctx, tenantID, jobID, "", limit)
}

func (s *Store) ListRunsFiltered(ctx context.Context, tenantID, jobID, broadcastGroupID string, limit int) ([]Run, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND (NULLIF($2,'') IS NULL OR job_id=NULLIF($2,'')::uuid) AND (NULLIF($3,'') IS NULL OR broadcast_group_id=NULLIF($3,'')::uuid) ORDER BY scheduled_at DESC,shard_index LIMIT $4`, tenantID, jobID, broadcastGroupID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.StartedAt, &r.FinishedAt, &r.ResponseStatus, &r.ErrorMessage, &r.RuntimeInput, &r.ParentRunID, &r.RetryOfRunID, &r.ExternalExecutionID, &r.ExecutorNodeID, &r.ExecutorAddress, &r.BroadcastGroupID, &r.ShardIndex, &r.ShardTotal, &r.RescheduleOnTerminal, &r.OverrideAddresses); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
