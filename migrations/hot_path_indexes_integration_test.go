//go:build integration

package migrations

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestHotPathIndexesMigration(t *testing.T) {
	container, err := postgres.Run(t.Context(), "postgres:16-alpine",
		postgres.WithDatabase("scheduler"),
		postgres.WithUsername("scheduler"),
		postgres.WithPassword("scheduler"),
		postgres.BasicWaitStrategies(),
	)
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
		if _, err = conn.Exec(t.Context(), migration.SQL); err != nil {
			t.Fatalf("apply migration %d: %v", migration.Version, err)
		}
	}
	want := []string{
		"job_runs_active_concurrency_idx",
		"job_runs_expired_lease_idx",
		"job_run_idempotency_created_idx",
		"job_run_idempotency_run_idx",
		"outbox_published_idx",
		"job_dependency_dispatches_created_idx",
		"job_dependency_dispatches_child_run_idx",
	}
	var count int
	if err = conn.QueryRow(t.Context(), `SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND indexname=ANY($1)`, want).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(want) {
		t.Fatalf("hot-path indexes = %d, want %d", count, len(want))
	}
	var tenantID, jobID string
	if err = conn.QueryRow(t.Context(), `INSERT INTO tenants(name) VALUES('index-plan') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO jobs(tenant_id,name,schedule_type,schedule_expression,timezone,target_url,http_method,headers,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled) VALUES($1,'index-plan','fixed_rate','60','UTC','http://example.invalid','POST','{}',30,0,'parallel','fire_once',false) RETURNING id`, tenantID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at)
		SELECT gen_random_uuid(),$1,$2,'manual','succeeded',now(),now() FROM generate_series(1,20000)`, tenantID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,lease_until)
		SELECT gen_random_uuid(),$1,$2,'manual','running',now(),now()+interval '1 minute' FROM generate_series(1,10)`, tenantID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `ANALYZE job_runs`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err = conn.QueryRow(t.Context(), `EXPLAIN (FORMAT JSON) SELECT job_id,tenant_id FROM job_runs WHERE (status='running' AND lease_until>=now()) OR status='waiting_callback'`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "Index") {
		t.Fatalf("active-run plan does not use an index: %s", plan)
	}
}
