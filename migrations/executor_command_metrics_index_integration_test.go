//go:build integration

package migrations

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestExecutorCommandMetricsIndexSupportsQueueStats(t *testing.T) {
	container, err := postgres.Run(t.Context(), "postgres:16-alpine", postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(t.Context(), "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(t.Context()) }()
	for _, migration := range All {
		if migration.Version >= 32 {
			break
		}
		if _, err = conn.Exec(t.Context(), migration.SQL); err != nil {
			t.Fatalf("migration %d: %v", migration.Version, err)
		}
	}
	var tenantID, jobID, runID, expectedExternalExecutionID string
	if err = conn.QueryRow(t.Context(), `INSERT INTO tenants(name) VALUES('executor-command-upgrade') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO jobs(tenant_id,name,schedule_type,schedule_expression,timezone,target_url,http_method,headers,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled) VALUES($1,'executor-command-upgrade','fixed_rate','60','UTC','http://example.invalid','POST','{}',30,0,'parallel','fire_once',false) RETURNING id`, tenantID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO job_runs(tenant_id,job_id,trigger_type,status,scheduled_at) VALUES($1,$2,'manual','cancelled',now()) RETURNING id,external_execution_id`, tenantID, jobID).Scan(&runID, &expectedExternalExecutionID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `INSERT INTO executor_commands(tenant_id,run_id,executor_address,command_type,payload) VALUES($1,$2,'executor:9999','cancel','{"reason":"upgrade"}')`, tenantID, runID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), ExecutorCommandMetricsIndexUp); err != nil {
		t.Fatal(err)
	}
	var externalExecutionID, payloadJobID string
	if err = conn.QueryRow(t.Context(), `SELECT payload->>'external_execution_id',payload->>'job_id' FROM executor_commands WHERE run_id=$1`, runID).Scan(&externalExecutionID, &payloadJobID); err != nil {
		t.Fatal(err)
	}
	if externalExecutionID != expectedExternalExecutionID || payloadJobID != jobID {
		t.Fatalf("backfilled executor command identity = external %q job %q", externalExecutionID, payloadJobID)
	}
	var definition string
	if err = conn.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='executor_commands_pending_created_idx'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(definition, "created_at") || !strings.Contains(definition, "WHERE (status = 'pending'::text)") {
		t.Fatalf("unexpected executor command metrics index: %s", definition)
	}
	if _, err = conn.Exec(t.Context(), `SET enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err = conn.QueryRow(t.Context(), `EXPLAIN (FORMAT JSON) SELECT count(*),COALESCE(GREATEST(EXTRACT(EPOCH FROM now()-min(created_at)),0),0) FROM executor_commands WHERE status='pending'`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "executor_commands_pending_created_idx") {
		t.Fatalf("executor command queue stats plan does not use pending index: %s", plan)
	}
}
