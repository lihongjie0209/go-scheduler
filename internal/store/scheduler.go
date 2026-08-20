package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
)

func (s *Store) EnqueueDue(ctx context.Context, batch int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, enqueueDueJobsQuery(), batch)
	if err != nil {
		return err
	}
	var jobs []Job
	for rows.Next() {
		j, e := s.scanJob(rows)
		if e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	routeStrategies, err := executorRouteStrategies(ctx, tx, jobs)
	if err != nil {
		return err
	}
	var fastBatch pgx.Batch
	for _, j := range jobs {
		if j.NextRunAt == nil {
			continue
		}
		now := time.Now().UTC()
		due, next, e := schedule.Due(j.ScheduleType, j.ScheduleExpression, j.Timezone, j.MisfirePolicy, *j.NextRunAt, now, int(j.MaxCatchUp))
		if e != nil {
			return e
		}
		routeStrategy := routeStrategies[j.ExecutorGroupID]
		fastPath := j.OverlapPolicy == "parallel" && routeStrategy != "sharding_broadcast"
		for _, scheduledAt := range due {
			if fastPath {
				runID := uuid.NewString()
				fastBatch.Queue(`INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule','pending',$4,$5,$1)`, runID, j.TenantID, j.ID, scheduledAt, j.ScheduleType == "fixed_delay")
				fastBatch.Queue(runLifecycleEventSQL, runID, "pending", "pending", uuid.NewString())
				continue
			}
			queued, hasActive, stateErr := jobRunQueueState(ctx, tx, j.ID)
			if stateErr != nil {
				return stateErr
			}
			action, actionErr := applyBlockAction(ctx, tx, j.ID, decideBlockAction(j.OverlapPolicy, hasActive))
			if actionErr != nil {
				return actionErr
			}
			status := "pending"
			if action == blockSkip || (action == blockEnqueue && queued >= int(j.MaxQueueSize)) {
				status = "skipped"
			}
			if routeStrategy == "sharding_broadcast" && status == "pending" {
				nodes, nodesErr := liveExecutorNodesTx(ctx, tx, j.ExecutorGroupID)
				if nodesErr != nil {
					return nodesErr
				}
				required, excluded, labelsErr := jobExecutorLabels(ctx, tx, j.ID)
				if labelsErr != nil {
					return labelsErr
				}
				nodes = FilterExecutorNodes(nodes, required, excluded)
				shards := planBroadcastShards(nodes)
				if len(shards) == 0 {
					return fmt.Errorf("no live executor nodes for broadcast job %s", j.ID)
				}
				if action == blockEnqueue && queued+len(shards) > int(j.MaxQueueSize) {
					status = "skipped"
				} else {
					groupID := uuid.NewString()
					for _, shard := range shards {
						runID := uuid.NewString()
						if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule','pending',$4,$5,$6,$7,$8,$9,$10,$1)`, runID, j.TenantID, j.ID, scheduledAt, shard.NodeID, shard.Address, groupID, shard.Index, shard.Total, j.ScheduleType == "fixed_delay"); err != nil {
							return err
						}
						if err = emitRunLifecycleEventTx(ctx, tx, runID, "pending"); err != nil {
							return err
						}
					}
					continue
				}
			}
			runID := uuid.NewString()
			_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,error_message,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule',$4,$5,CASE WHEN $4='skipped' THEN now() END,CASE WHEN $4='skipped' THEN 'block strategy: discard later or queue full' ELSE NULL END,$6,$1)`, runID, j.TenantID, j.ID, status, scheduledAt, j.ScheduleType == "fixed_delay")
			if err != nil {
				return err
			}
			if err = emitRunLifecycleEventTx(ctx, tx, runID, status); err != nil {
				return err
			}
			if j.ScheduleType == "fixed_delay" && status == "skipped" {
				next, err = schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, now)
				if err != nil {
					return err
				}
			}
		}
		var arg any
		if !next.IsZero() {
			arg = next
		}
		if fastPath {
			fastBatch.Queue(`UPDATE jobs SET last_run_at=next_run_at,next_run_at=$2,enabled=CASE WHEN $2::timestamptz IS NULL AND schedule_type='once' THEN false ELSE enabled END,updated_at=now() WHERE id=$1`, j.ID, arg)
		} else {
			_, err = tx.Exec(ctx, `UPDATE jobs SET last_run_at=next_run_at,next_run_at=$2,enabled=CASE WHEN $2::timestamptz IS NULL AND schedule_type='once' THEN false ELSE enabled END,updated_at=now() WHERE id=$1`, j.ID, arg)
			if err != nil {
				return err
			}
		}
	}
	if fastBatch.Len() > 0 {
		results := tx.SendBatch(ctx, &fastBatch)
		for index := 0; index < fastBatch.Len(); index++ {
			if _, err = results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("execute due-run batch: %w", err)
			}
		}
		if err = results.Close(); err != nil {
			return fmt.Errorf("close due-run batch: %w", err)
		}
	}
	return tx.Commit(ctx)
}

type ClaimedRun struct {
	Run Run
	Job Job
}

const lockKubernetesClaimCapacitySQL = `SELECT kc.id FROM kubernetes_clusters kc
	WHERE EXISTS (SELECT 1 FROM jobs j JOIN job_runs r ON r.job_id=j.id WHERE j.kubernetes_cluster_id=kc.id AND ((r.status='pending' AND r.available_at<=now()) OR (r.status='running' AND r.lease_until<now())))
	ORDER BY kc.id FOR UPDATE OF kc`

const claimRunsSQL = `WITH active AS MATERIALIZED (
	 SELECT job_id,tenant_id FROM job_runs WHERE (status='running' AND lease_until>=now()) OR status='waiting_callback'
	), active_job AS (
	 SELECT job_id,count(*) AS n FROM active GROUP BY job_id
	), active_tenant AS (
	 SELECT tenant_id,count(*) AS n FROM active GROUP BY tenant_id
	), active_cluster AS (
	 SELECT j.kubernetes_cluster_id,count(*) AS n FROM jobs j JOIN active a ON a.job_id=j.id WHERE j.kubernetes_cluster_id IS NOT NULL GROUP BY j.kubernetes_cluster_id
	), candidates AS (
	 SELECT r.id,r.job_id,r.tenant_id,
	 row_number() OVER(PARTITION BY r.job_id ORDER BY r.available_at,r.id) AS job_rank,
	 row_number() OVER(PARTITION BY r.tenant_id ORDER BY r.available_at,r.id) AS tenant_rank,
	 CASE WHEN j.kubernetes_cluster_id IS NULL THEN 1 ELSE row_number() OVER(PARTITION BY j.kubernetes_cluster_id ORDER BY r.available_at,r.id) END AS cluster_rank,
		 CASE WHEN r.broadcast_group_id IS NOT NULL THEN GREATEST(j.max_concurrent_runs,r.shard_total) ELSE j.max_concurrent_runs END AS max_concurrent_runs,t.max_concurrent_runs AS tenant_max,j.timeout_seconds,
	 j.kubernetes_cluster_id,COALESCE(kc.max_concurrent_jobs,1) AS cluster_max,COALESCE(ac.n,0) AS cluster_active,
	 COALESCE(aj.n,0) AS job_active,COALESCE(at.n,0) AS tenant_active
	 FROM job_runs r JOIN jobs j ON j.id=r.job_id JOIN tenants t ON t.id=r.tenant_id
	 LEFT JOIN active_job aj ON aj.job_id=r.job_id LEFT JOIN active_tenant at ON at.tenant_id=r.tenant_id
	 LEFT JOIN kubernetes_clusters kc ON kc.id=j.kubernetes_cluster_id LEFT JOIN active_cluster ac ON ac.kubernetes_cluster_id=j.kubernetes_cluster_id
	 WHERE (r.status='pending' AND r.available_at<=now()) OR (r.status='running' AND r.lease_until<now())
	), eligible AS (
	 SELECT r.id,c.timeout_seconds,r.status='pending' AS emit_running FROM job_runs r JOIN candidates c ON c.id=r.id
	 WHERE c.job_rank<=GREATEST(c.max_concurrent_runs-c.job_active,0)
	 AND c.tenant_rank<=GREATEST(c.tenant_max-c.tenant_active,0)
	 AND (c.kubernetes_cluster_id IS NULL OR c.cluster_rank<=GREATEST(c.cluster_max-c.cluster_active,0))
	 ORDER BY r.available_at,r.id FOR UPDATE OF r SKIP LOCKED LIMIT $1
	), claimed AS (UPDATE job_runs r SET status='running',lease_owner=$2,lease_token=gen_random_uuid(),lease_until=now()+GREATEST($3,eligible.timeout_seconds+30)*interval '1 second',started_at=COALESCE(started_at,now()) FROM eligible WHERE r.id=eligible.id AND ((r.status='pending' AND r.available_at<=now()) OR (r.status='running' AND r.lease_until<now())) RETURNING r.id,r.tenant_id,r.job_id,r.trigger_type,r.status,r.attempt,r.scheduled_at,r.runtime_input,r.parent_run_id,r.retry_of_run_id,r.external_execution_id,r.executor_node_id,r.executor_address,r.broadcast_group_id,r.shard_index,r.shard_total,r.reschedule_on_terminal,r.override_addresses,r.lease_token,eligible.emit_running
	), emitted AS (INSERT INTO outbox_events(id,tenant_id,topic,payload)
	 SELECT gen_random_uuid(),c.tenant_id,'job.run.running',jsonb_build_object('run_id',c.id::text,'job_id',c.job_id::text,'tenant_id',c.tenant_id::text,'status','running','attempt',c.attempt,'trigger_type',c.trigger_type,'scheduled_at',c.scheduled_at,'occurred_at',now())
	 FROM claimed c WHERE c.emit_running)
	 SELECT c.id,c.tenant_id,c.job_id,c.trigger_type,c.status,c.attempt,c.scheduled_at,c.runtime_input,COALESCE(c.parent_run_id::text,''),COALESCE(c.retry_of_run_id::text,''),c.external_execution_id,COALESCE(c.executor_node_id,''),COALESCE(c.executor_address,''),COALESCE(c.broadcast_group_id::text,''),COALESCE(c.shard_index,0),COALESCE(c.shard_total,0),c.reschedule_on_terminal,c.override_addresses,c.lease_token,`

func (s *Store) ClaimRuns(ctx context.Context, owner string, limit int, lease time.Duration) ([]ClaimedRun, error) {
	connection, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Release()
	// Send the transaction start, deterministic capacity locks, and claim as one
	// protocol batch. Claim remains a separate READ COMMITTED statement, so it
	// sees work committed by a competing Core before that Core released the lock.
	batch := &pgx.Batch{}
	batch.Queue("BEGIN")
	batch.Queue(lockKubernetesClaimCapacitySQL)
	batch.Queue(claimRunsSQL+jobColumnsWithAlias("j")+` FROM claimed c JOIN jobs j ON j.id=c.job_id`, limit, owner, lease.Seconds())
	results := connection.SendBatch(ctx, batch)
	transactionOpen := false
	defer func() {
		_ = results.Close()
		if transactionOpen {
			rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_, _ = connection.Exec(rollbackCtx, "ROLLBACK")
		}
	}()
	if _, err = results.Exec(); err != nil {
		return nil, fmt.Errorf("begin run claim: %w", err)
	}
	transactionOpen = true
	clusterRows, err := results.Query()
	if err != nil {
		return nil, fmt.Errorf("lock Kubernetes cluster capacity: %w", err)
	}
	for clusterRows.Next() {
		var clusterID string
		if err = clusterRows.Scan(&clusterID); err != nil {
			clusterRows.Close()
			return nil, err
		}
	}
	err = clusterRows.Err()
	clusterRows.Close()
	if err != nil {
		return nil, err
	}
	rows, err := results.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedRun
	for rows.Next() {
		var x ClaimedRun
		var headers, encrypted, dockerAuth []byte
		var keyVersion, dockerAuthKeyVersion *int
		err = rows.Scan(&x.Run.ID, &x.Run.TenantID, &x.Run.JobID, &x.Run.TriggerType, &x.Run.Status, &x.Run.Attempt, &x.Run.ScheduledAt, &x.Run.RuntimeInput, &x.Run.ParentRunID, &x.Run.RetryOfRunID, &x.Run.ExternalExecutionID, &x.Run.ExecutorNodeID, &x.Run.ExecutorAddress, &x.Run.BroadcastGroupID, &x.Run.ShardIndex, &x.Run.ShardTotal, &x.Run.RescheduleOnTerminal, &x.Run.OverrideAddresses, &x.Run.LeaseToken, &x.Job.ID, &x.Job.TenantID, &x.Job.Name, &x.Job.Description, &x.Job.ScheduleType, &x.Job.ScheduleExpression, &x.Job.Timezone, &x.Job.TargetURL, &x.Job.HTTPMethod, &headers, &encrypted, &keyVersion, &dockerAuth, &dockerAuthKeyVersion, &x.Job.BodyTemplate, &x.Job.TimeoutSeconds, &x.Job.MaxRetries, &x.Job.OverlapPolicy, &x.Job.MisfirePolicy, &x.Job.Enabled, &x.Job.NextRunAt, &x.Job.Version, &x.Job.MaxConcurrentRuns, &x.Job.MaxCatchUp, &x.Job.CallbackTimeoutSeconds, &x.Job.MaxQueueSize, &x.Job.ExecutorGroupID, &x.Job.ExecutorHandler, &x.Job.ScriptLanguage, &x.Job.ScriptSource, &x.Job.KubernetesClusterID)
		if err != nil {
			return nil, err
		}
		if len(encrypted) > 0 {
			if s.headerCipher == nil || keyVersion == nil {
				return nil, fmt.Errorf("encrypted headers require store cipher")
			}
			headers, err = s.headerCipher.Decrypt(encrypted, *keyVersion)
			if err != nil {
				return nil, err
			}
		}
		if err = json.Unmarshal(headers, &x.Job.Headers); err != nil {
			return nil, err
		}
		if err = s.decryptDockerRegistryAuth(dockerAuth, dockerAuthKeyVersion, &x.Job.DockerRegistryAuth); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err = results.Close(); err != nil {
		return nil, err
	}
	if _, err = connection.Exec(ctx, "COMMIT"); err != nil {
		return nil, err
	}
	transactionOpen = false
	return out, nil
}
func jobColumnsWithAlias(a string) string {
	return a + `.id,` + a + `.tenant_id,` + a + `.name,` + a + `.description,` + a + `.schedule_type,` + a + `.schedule_expression,` + a + `.timezone,` + a + `.target_url,` + a + `.http_method,` + a + `.headers,` + a + `.encrypted_headers,` + a + `.encryption_key_version,` + a + `.encrypted_docker_registry_auth,` + a + `.docker_registry_auth_key_version,` + a + `.body_template,` + a + `.timeout_seconds,` + a + `.max_retries,` + a + `.overlap_policy,` + a + `.misfire_policy,` + a + `.enabled,` + a + `.next_run_at,` + a + `.version,` + a + `.max_concurrent_runs,` + a + `.max_catch_up,` + a + `.callback_timeout_seconds,` + a + `.max_queue_size,COALESCE(` + a + `.executor_group_id::text,''),` + a + `.executor_handler,` + a + `.script_language,` + a + `.script_source,COALESCE(` + a + `.kubernetes_cluster_id::text,'')`
}
