package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
)

func (s *Store) CompleteRun(ctx context.Context, r Run, success bool, status int, body, errorMessage string) error {
	if err := requireRunLease(r); err != nil {
		return err
	}
	state := "succeeded"
	if !success {
		state = "failed"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var finishedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE job_runs SET status=$2,response_status=$3,response_body=$4,error_message=$5,finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='running' AND lease_token=$6 RETURNING finished_at`, r.ID, state, status, body, errorMessage, r.LeaseToken).Scan(&finishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, r.ID, state); err != nil {
		return err
	}
	if !success {
		if err = emitRunEventTx(ctx, tx, r.ID, state, "exhausted"); err != nil {
			return err
		}
	}
	if success {
		if err = enqueueDependentRuns(ctx, tx, r); err != nil {
			return err
		}
	}
	if err = rearmFixedDelay(ctx, tx, r, finishedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func rearmFixedDelay(ctx context.Context, tx pgx.Tx, r Run, terminalAt time.Time) error {
	if !r.RescheduleOnTerminal {
		return nil
	}
	if r.BroadcastGroupID != "" {
		var unfinished bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE broadcast_group_id=$1 AND reschedule_on_terminal AND status IN ('pending','running','waiting_callback'))`, r.BroadcastGroupID).Scan(&unfinished); err != nil {
			return err
		}
		if unfinished {
			return nil
		}
	}
	var expression, timezone string
	var enabled bool
	var existingNext *time.Time
	err := tx.QueryRow(ctx, `SELECT schedule_expression,timezone,enabled,next_run_at FROM jobs WHERE id=$1 AND schedule_type='fixed_delay' FOR UPDATE`, r.JobID).Scan(&expression, &timezone, &enabled, &existingNext)
	if errors.Is(err, pgx.ErrNoRows) || !enabled || existingNext != nil {
		return nil
	}
	if err != nil {
		return err
	}
	next, err := schedule.Next("fixed_delay", expression, timezone, terminalAt)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET next_run_at=$2,last_run_at=$3,updated_at=now() WHERE id=$1 AND enabled AND schedule_type='fixed_delay' AND next_run_at IS NULL`, r.JobID, next, terminalAt)
	return err
}

func enqueueDependentRuns(ctx context.Context, tx pgx.Tx, parent Run) error {
	rows, err := tx.Query(ctx, `SELECT d.child_job_id,j.overlap_policy,j.max_queue_size,COALESCE(j.executor_group_id::text,''),COALESCE(g.route_strategy,'') FROM job_dependencies d JOIN jobs j ON j.id=d.child_job_id LEFT JOIN executor_groups g ON g.id=j.executor_group_id WHERE d.tenant_id=$1 AND d.parent_job_id=$2 ORDER BY d.child_job_id`, parent.TenantID, parent.JobID)
	if err != nil {
		return fmt.Errorf("query dependent jobs: %w", err)
	}
	type dependent struct {
		id, policy, executorGroupID, routeStrategy string
		maxQueue                                   int
	}
	var children []dependent
	for rows.Next() {
		var child dependent
		if err = rows.Scan(&child.id, &child.policy, &child.maxQueue, &child.executorGroupID, &child.routeStrategy); err != nil {
			rows.Close()
			return err
		}
		children = append(children, child)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, child := range children {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, child.id); err != nil {
			return err
		}
		runID := uuid.NewString()
		scheduledAt := time.Now().UTC()
		tag, reserveErr := tx.Exec(ctx, `INSERT INTO job_dependency_dispatches(parent_run_id,child_job_id,child_run_id,child_scheduled_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, parent.ID, child.id, runID, scheduledAt)
		if reserveErr != nil {
			return reserveErr
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		var pending int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1 AND status='pending'`, child.id).Scan(&pending); err != nil {
			return err
		}
		action, actionErr := applyBlockPolicy(ctx, tx, child.id, child.policy)
		if actionErr != nil {
			return actionErr
		}
		state := "pending"
		message := ""
		if action == blockSkip || (action == blockEnqueue && pending >= child.maxQueue) {
			state = "skipped"
			message = "dependency trigger skipped by block strategy or queue limit"
		}
		if child.routeStrategy == "sharding_broadcast" && state == "pending" {
			nodes, nodesErr := liveExecutorNodesTx(ctx, tx, child.executorGroupID)
			if nodesErr != nil {
				return nodesErr
			}
			required, excluded, labelsErr := jobExecutorLabels(ctx, tx, child.id)
			if labelsErr != nil {
				return labelsErr
			}
			nodes = FilterExecutorNodes(nodes, required, excluded)
			shards := planBroadcastShards(nodes)
			if len(shards) == 0 {
				return fmt.Errorf("no live executor nodes for dependent broadcast job %s", child.id)
			}
			if action == blockEnqueue && pending+len(shards) > child.maxQueue {
				state = "skipped"
				message = "dependency trigger skipped by queue limit"
			} else {
				groupID := uuid.NewString()
				for index, shard := range shards {
					shardRunID := uuid.NewString()
					if index == 0 {
						shardRunID = runID
					}
					if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,parent_run_id,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,external_execution_id) VALUES($1,$2,$3,'dependency','pending',$4,$5,$6,$7,$8,$9,$10,$1)`, shardRunID, parent.TenantID, child.id, scheduledAt, parent.ID, shard.NodeID, shard.Address, groupID, shard.Index, shard.Total); err != nil {
						return err
					}
					if err = emitRunLifecycleEventTx(ctx, tx, shardRunID, "pending"); err != nil {
						return err
					}
				}
				continue
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,error_message,parent_run_id,external_execution_id) VALUES($1,$2,$3,'dependency',$4,$5,CASE WHEN $4='skipped' THEN now() END,NULLIF($6,''),$7,$1)`, runID, parent.TenantID, child.id, state, scheduledAt, message, parent.ID); err != nil {
			return err
		}
		if err = emitRunLifecycleEventTx(ctx, tx, runID, state); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkClaimedWaitingCallback(ctx context.Context, run Run, status int, tokenHash []byte, deadline time.Time) error {
	if err := requireRunLease(run); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE job_runs SET status='waiting_callback',response_status=$2,callback_token_hash=$3,callback_deadline=$4,lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND status='running' AND lease_token=$5`, run.ID, status, tokenHash, deadline, run.LeaseToken)
	if err != nil {
		return fmt.Errorf("mark waiting callback: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = emitRunLifecycleEventTx(ctx, tx, run.ID, "waiting_callback"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CompleteCallback(ctx context.Context, runID string, tokenHash []byte, succeeded bool, message string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var terminal Run
	var maxRetries int32
	err = tx.QueryRow(ctx, `WITH updated AS (
	 UPDATE job_runs SET status=CASE WHEN $3 THEN 'succeeded' ELSE 'failed' END,error_message=CASE WHEN $3 THEN '' ELSE $4 END,finished_at=now(),callback_consumed_at=now(),callback_token_hash=NULL,callback_deadline=NULL
	 WHERE id=$1 AND status='waiting_callback' AND callback_token_hash=$2 AND callback_deadline>now() RETURNING *
	) SELECT `+runColumns("u")+`,j.max_retries FROM updated u JOIN jobs j ON j.id=u.job_id`, runID, tokenHash, succeeded, message).Scan(&terminal.ID, &terminal.TenantID, &terminal.JobID, &terminal.TriggerType, &terminal.Status, &terminal.Attempt, &terminal.ScheduledAt, &terminal.StartedAt, &terminal.FinishedAt, &terminal.ResponseStatus, &terminal.ErrorMessage, &terminal.RuntimeInput, &terminal.ParentRunID, &terminal.RetryOfRunID, &terminal.ExternalExecutionID, &terminal.ExecutorNodeID, &terminal.ExecutorAddress, &terminal.BroadcastGroupID, &terminal.ShardIndex, &terminal.ShardTotal, &terminal.RescheduleOnTerminal, &terminal.OverrideAddresses, &maxRetries)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("complete callback: %w", err)
	}
	finishedAt := *terminal.FinishedAt
	if err = emitRunLifecycleEventTx(ctx, tx, runID, terminal.Status); err != nil {
		return err
	}
	willRetry := !succeeded && terminal.Attempt <= maxRetries
	if !succeeded && !willRetry {
		if err = emitRunEventTx(ctx, tx, runID, terminal.Status, "exhausted"); err != nil {
			return err
		}
	}
	if willRetry {
		if _, err = insertRetryRunTx(ctx, tx, terminal, callbackRetryDelay(terminal.Attempt)); err != nil {
			return err
		}
	} else if succeeded {
		if err = enqueueDependentRuns(ctx, tx, Run{ID: runID, TenantID: terminal.TenantID, JobID: terminal.JobID}); err != nil {
			return err
		}
	}
	if !willRetry {
		if err = rearmFixedDelay(ctx, tx, terminal, finishedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func callbackRetryDelay(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 6 {
		shift = 6
	}
	return time.Second * time.Duration(1<<shift)
}

func insertRetryRunTx(ctx context.Context, tx pgx.Tx, previous Run, delay time.Duration) (Run, error) {
	next := Run{ID: uuid.NewString(), TenantID: previous.TenantID, JobID: previous.JobID, TriggerType: "retry", Status: "pending", RuntimeInput: previous.RuntimeInput, ParentRunID: previous.ParentRunID, RetryOfRunID: previous.ID, Attempt: previous.Attempt + 1, ScheduledAt: time.Now().UTC(), ExecutorNodeID: previous.ExecutorNodeID, ExecutorAddress: previous.ExecutorAddress, BroadcastGroupID: previous.BroadcastGroupID, ShardIndex: previous.ShardIndex, ShardTotal: previous.ShardTotal, RescheduleOnTerminal: previous.RescheduleOnTerminal, OverrideAddresses: previous.OverrideAddresses}
	next.ExternalExecutionID = next.ID
	_, err := tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,available_at,runtime_input,parent_run_id,retry_of_run_id,external_execution_id,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,reschedule_on_terminal,override_addresses) VALUES($1,$2,$3,'retry','pending',$4,$5::timestamptz,$5::timestamptz+$6*interval '1 second',$7,NULLIF($8,'')::uuid,$9,$10,NULLIF($11,''),NULLIF($12,''),NULLIF($13,'')::uuid,CASE WHEN $13='' THEN NULL ELSE $14::integer END,CASE WHEN $13='' THEN NULL ELSE $15::integer END,$16,$17)`, next.ID, next.TenantID, next.JobID, next.Attempt, next.ScheduledAt, delay.Seconds(), next.RuntimeInput, next.ParentRunID, next.RetryOfRunID, next.ExternalExecutionID, next.ExecutorNodeID, next.ExecutorAddress, next.BroadcastGroupID, next.ShardIndex, next.ShardTotal, next.RescheduleOnTerminal, next.OverrideAddresses)
	if err != nil {
		return Run{}, err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, next.ID, "pending"); err != nil {
		return Run{}, err
	}
	return next, nil
}

func runColumns(alias string) string {
	return alias + `.id,` + alias + `.tenant_id,` + alias + `.job_id,` + alias + `.trigger_type,` + alias + `.status,` + alias + `.attempt,` + alias + `.scheduled_at,` + alias + `.started_at,` + alias + `.finished_at,COALESCE(` + alias + `.response_status,0),COALESCE(` + alias + `.error_message,''),` + alias + `.runtime_input,COALESCE(` + alias + `.parent_run_id::text,''),COALESCE(` + alias + `.retry_of_run_id::text,''),` + alias + `.external_execution_id,COALESCE(` + alias + `.executor_node_id,''),COALESCE(` + alias + `.executor_address,''),COALESCE(` + alias + `.broadcast_group_id::text,''),COALESCE(` + alias + `.shard_index,0),COALESCE(` + alias + `.shard_total,0),` + alias + `.reschedule_on_terminal,` + alias + `.override_addresses`
}

const callbackExpiryBatchSize = 200

func enqueueDueJobsQuery() string {
	return `SELECT ` + jobEnqueueColumns + ` FROM jobs WHERE enabled AND next_run_at<=now() ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT $1`
}

func expireCallbacksQuery() string {
	return `SELECT ` + runColumns("r") + `,j.max_retries FROM job_runs r JOIN jobs j ON j.id=r.job_id WHERE r.status='waiting_callback' AND r.callback_deadline<=now() FOR UPDATE OF r SKIP LOCKED LIMIT $1`
}

func (s *Store) ExpireCallbacks(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, expireCallbacksQuery(), callbackExpiryBatchSize)
	if err != nil {
		return err
	}
	type expiredRun struct {
		run        Run
		maxRetries int32
	}
	var expired []expiredRun
	for rows.Next() {
		var item expiredRun
		if err = rows.Scan(&item.run.ID, &item.run.TenantID, &item.run.JobID, &item.run.TriggerType, &item.run.Status, &item.run.Attempt, &item.run.ScheduledAt, &item.run.StartedAt, &item.run.FinishedAt, &item.run.ResponseStatus, &item.run.ErrorMessage, &item.run.RuntimeInput, &item.run.ParentRunID, &item.run.RetryOfRunID, &item.run.ExternalExecutionID, &item.run.ExecutorNodeID, &item.run.ExecutorAddress, &item.run.BroadcastGroupID, &item.run.ShardIndex, &item.run.ShardTotal, &item.run.RescheduleOnTerminal, &item.run.OverrideAddresses, &item.maxRetries); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, item := range expired {
		var finishedAt time.Time
		if err = tx.QueryRow(ctx, `UPDATE job_runs SET status='timed_out',finished_at=now(),error_message='callback deadline exceeded',callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='waiting_callback' RETURNING finished_at`, item.run.ID).Scan(&finishedAt); err != nil {
			return err
		}
		if err = emitRunLifecycleEventTx(ctx, tx, item.run.ID, "timed_out"); err != nil {
			return err
		}
		if item.run.Attempt <= item.maxRetries {
			if _, err = insertRetryRunTx(ctx, tx, item.run, callbackRetryDelay(item.run.Attempt)); err != nil {
				return err
			}
		} else {
			if err = emitRunEventTx(ctx, tx, item.run.ID, "timed_out", "exhausted"); err != nil {
				return err
			}
			if err = rearmFixedDelay(ctx, tx, item.run, finishedAt); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}
