package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type blockAction string

const (
	blockEnqueue          blockAction = "enqueue"
	blockSkip             blockAction = "skip"
	blockCancelAndEnqueue blockAction = "cancel_and_enqueue"
)

func canonicalBlockPolicy(policy string) string {
	switch policy {
	case "queue":
		return "serial"
	case "skip":
		return "discard_later"
	default:
		return policy
	}
}

func decideBlockAction(policy string, hasActive bool) blockAction {
	if !hasActive {
		return blockEnqueue
	}
	switch canonicalBlockPolicy(policy) {
	case "discard_later":
		return blockSkip
	case "cover_early":
		return blockCancelAndEnqueue
	default:
		return blockEnqueue
	}
}

func (s *Store) TriggerJob(ctx context.Context, tenantID, jobID, key, input string) (Run, error) {
	return s.TriggerJobWithOptions(ctx, tenantID, jobID, key, input, TriggerOptions{})
}

type TriggerOptions struct {
	OverrideAddresses []string
}

func (s *Store) TriggerJobWithOptions(ctx context.Context, tenantID, jobID, key, input string, options TriggerOptions) (Run, error) {
	if options.OverrideAddresses == nil {
		options.OverrideAddresses = []string{}
	}
	r := Run{ID: uuid.NewString(), TenantID: tenantID, JobID: jobID, TriggerType: "manual", Status: "pending", Attempt: 1, ScheduledAt: time.Now().UTC(), RuntimeInput: input, OverrideAddresses: options.OverrideAddresses}
	r.ExternalExecutionID = r.ID
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("begin trigger: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, jobID); err != nil {
		return Run{}, fmt.Errorf("lock job queue: %w", err)
	}
	var maxQueue, pending int
	var blockPolicy, routeStrategy, executorGroupID string
	err = tx.QueryRow(ctx, `SELECT j.max_queue_size,j.overlap_policy,COALESCE(g.route_strategy,''),COALESCE(j.executor_group_id::text,''),(SELECT count(*) FROM job_runs WHERE job_id=$1 AND status='pending') FROM jobs j LEFT JOIN executor_groups g ON g.id=j.executor_group_id WHERE j.id=$1 AND j.tenant_id=$2`, jobID, tenantID).Scan(&maxQueue, &blockPolicy, &routeStrategy, &executorGroupID, &pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("check job queue: %w", err)
	}
	if len(options.OverrideAddresses) > 0 && executorGroupID == "" {
		return Run{}, ErrOverrideRequiresExecutorGroup
	}
	if key != "" {
		var inserted string
		err = tx.QueryRow(ctx, `INSERT INTO job_run_idempotency(tenant_id,job_id,idempotency_key,run_id,scheduled_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING run_id`, tenantID, jobID, key, r.ID, r.ScheduledAt).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			r, err = scanRun(tx.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE (id,scheduled_at)=(SELECT run_id,scheduled_at FROM job_run_idempotency WHERE tenant_id=$1 AND job_id=$2 AND idempotency_key=$3)`, tenantID, jobID, key))
			if err != nil {
				return Run{}, fmt.Errorf("read idempotent run: %w", err)
			}
			if err = tx.Commit(ctx); err != nil {
				return Run{}, err
			}
			return r, nil
		}
		if err != nil {
			return Run{}, fmt.Errorf("reserve idempotency key: %w", err)
		}
	}
	if len(options.OverrideAddresses) > 0 {
		nodes, nodesErr := liveExecutorNodesTx(ctx, tx, executorGroupID)
		if nodesErr != nil {
			return Run{}, nodesErr
		}
		if missing := unregisteredOverrideAddresses(nodes, options.OverrideAddresses); len(missing) > 0 {
			return Run{}, fmt.Errorf("%w: %s", ErrOverrideAddressNotRegistered, strings.Join(missing, ", "))
		}
	}
	action, err := applyBlockPolicy(ctx, tx, jobID, blockPolicy)
	if err != nil {
		return Run{}, err
	}
	if action == blockEnqueue && pending >= maxQueue {
		return Run{}, ErrQueueFull
	}
	status := "pending"
	if action == blockSkip {
		status = "skipped"
	}
	if routeStrategy == "sharding_broadcast" && status == "pending" {
		var nodes []ExecutorNode
		if len(options.OverrideAddresses) > 0 {
			nodes = OverrideExecutorNodes(options.OverrideAddresses)
		} else {
			var nodesErr error
			nodes, nodesErr = liveExecutorNodesTx(ctx, tx, executorGroupID)
			if nodesErr != nil {
				return Run{}, nodesErr
			}
			required, excluded, labelsErr := jobExecutorLabels(ctx, tx, jobID)
			if labelsErr != nil {
				return Run{}, labelsErr
			}
			nodes = FilterExecutorNodes(nodes, required, excluded)
		}
		shards := planBroadcastShards(nodes)
		if len(shards) == 0 {
			return Run{}, fmt.Errorf("no live executor nodes")
		}
		if action == blockEnqueue && pending+len(shards) > maxQueue {
			return Run{}, ErrQueueFull
		}
		r.BroadcastGroupID = uuid.NewString()
		for index, shard := range shards {
			runID := uuid.NewString()
			idempotencyKey := ""
			if index == 0 {
				runID = r.ID
				idempotencyKey = key
			}
			_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,runtime_input,idempotency_key,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,override_addresses,external_execution_id) VALUES($1,$2,$3,'manual','pending',$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,$12,$1)`, runID, tenantID, jobID, r.ScheduledAt, input, idempotencyKey, shard.NodeID, shard.Address, r.BroadcastGroupID, shard.Index, shard.Total, options.OverrideAddresses)
			if err != nil {
				return Run{}, fmt.Errorf("insert broadcast shard: %w", err)
			}
			if err = emitRunLifecycleEventTx(ctx, tx, runID, "pending"); err != nil {
				return Run{}, err
			}
		}
		r.ShardIndex = 0
		r.ShardTotal = int32(len(shards)) // #nosec G115 -- executor addresses are validated to at most 100 entries.
		r.ExecutorNodeID = shards[0].NodeID
		r.ExecutorAddress = shards[0].Address
	} else {
		err = tx.QueryRow(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,runtime_input,idempotency_key,error_message,override_addresses,external_execution_id) VALUES($1,$2,$3,'manual',$4,$5,CASE WHEN $4='skipped' THEN now() END,$6,NULLIF($7,''),CASE WHEN $4='skipped' THEN 'block strategy: discard later' ELSE NULL END,$8,$1) RETURNING id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,runtime_input`, r.ID, tenantID, jobID, status, r.ScheduledAt, input, key, options.OverrideAddresses).Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.RuntimeInput)
		if err != nil {
			return Run{}, fmt.Errorf("insert run: %w", err)
		}
		if err = emitRunLifecycleEventTx(ctx, tx, r.ID, status); err != nil {
			return Run{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit trigger: %w", err)
	}
	return r, nil
}

func applyBlockPolicy(ctx context.Context, tx pgx.Tx, jobID, policy string) (blockAction, error) {
	var hasActive bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE job_id=$1 AND status IN ('pending','running','waiting_callback'))`, jobID).Scan(&hasActive); err != nil {
		return "", fmt.Errorf("check active job runs: %w", err)
	}
	action := decideBlockAction(policy, hasActive)
	return applyBlockAction(ctx, tx, jobID, action)
}

func applyBlockAction(ctx context.Context, tx pgx.Tx, jobID string, action blockAction) (blockAction, error) {
	if action == blockCancelAndEnqueue {
		rows, err := tx.Query(ctx, `UPDATE job_runs SET status='cancelled',finished_at=now(),error_message='block strategy: covered by newer trigger',lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE job_id=$1 AND status IN ('pending','running','waiting_callback') RETURNING id,tenant_id,job_id,COALESCE(broadcast_group_id::text,''),reschedule_on_terminal,finished_at,COALESCE(executor_address,''),external_execution_id`, jobID)
		if err != nil {
			return "", fmt.Errorf("cancel covered job runs: %w", err)
		}
		type coveredRun struct {
			run        Run
			finishedAt time.Time
		}
		var covered []coveredRun
		for rows.Next() {
			var item coveredRun
			if err = rows.Scan(&item.run.ID, &item.run.TenantID, &item.run.JobID, &item.run.BroadcastGroupID, &item.run.RescheduleOnTerminal, &item.finishedAt, &item.run.ExecutorAddress, &item.run.ExternalExecutionID); err != nil {
				rows.Close()
				return "", err
			}
			covered = append(covered, item)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return "", err
		}
		for _, item := range covered {
			if err = emitRunLifecycleEventTx(ctx, tx, item.run.ID, "cancelled"); err != nil {
				return "", err
			}
			if item.run.ExecutorAddress != "" {
				if err = enqueueExecutorCancellationTx(ctx, tx, item.run, "block strategy: covered by newer trigger"); err != nil {
					return "", err
				}
			}
			if err = rearmFixedDelay(ctx, tx, item.run, item.finishedAt); err != nil {
				return "", err
			}
		}
	}
	return action, nil
}

func jobRunQueueState(ctx context.Context, tx pgx.Tx, jobID string) (int, bool, error) {
	var queued int
	var hasActive bool
	err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='pending'),count(*) FILTER (WHERE status IN ('pending','running','waiting_callback'))>0 FROM job_runs WHERE job_id=$1`, jobID).Scan(&queued, &hasActive)
	if err != nil {
		return 0, false, fmt.Errorf("query job run queue state: %w", err)
	}
	return queued, hasActive, nil
}

func executorRouteStrategies(ctx context.Context, tx pgx.Tx, jobs []Job) (map[string]string, error) {
	groupIDs := make([]string, 0, len(jobs))
	seen := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		if job.ExecutorGroupID == "" {
			continue
		}
		if _, ok := seen[job.ExecutorGroupID]; ok {
			continue
		}
		seen[job.ExecutorGroupID] = struct{}{}
		groupIDs = append(groupIDs, job.ExecutorGroupID)
	}
	strategies := make(map[string]string, len(groupIDs))
	if len(groupIDs) == 0 {
		return strategies, nil
	}
	rows, err := tx.Query(ctx, `SELECT id::text,route_strategy FROM executor_groups WHERE id=ANY($1::uuid[])`, groupIDs)
	if err != nil {
		return nil, fmt.Errorf("query executor route strategies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, strategy string
		if err = rows.Scan(&id, &strategy); err != nil {
			return nil, err
		}
		strategies[id] = strategy
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(strategies) != len(groupIDs) {
		return nil, ErrNotFound
	}
	return strategies, nil
}
