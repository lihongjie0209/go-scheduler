//go:build integration

package migrations

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestExternalExecutionIdentityMigrationBackfillsExistingRows(t *testing.T) {
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
		if migration.Version >= 30 {
			break
		}
		if _, err = conn.Exec(t.Context(), migration.SQL); err != nil {
			t.Fatalf("apply migration %d: %v", migration.Version, err)
		}
	}
	var tenantID, jobID, runID string
	if err = conn.QueryRow(t.Context(), `INSERT INTO tenants(name) VALUES('identity-upgrade') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO jobs(tenant_id,name,schedule_type,schedule_expression,timezone,target_url,http_method,headers,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled) VALUES($1,'identity-upgrade','fixed_rate','60','UTC','http://example.invalid','POST','{}',30,0,'parallel','fire_once',false) RETURNING id`, tenantID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO job_runs(tenant_id,job_id,trigger_type,status,scheduled_at) VALUES($1,$2,'manual','pending',now()) RETURNING id`, tenantID, jobID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), ExternalExecutionIdentityUp); err != nil {
		t.Fatal(err)
	}
	var executionID string
	if err = conn.QueryRow(t.Context(), `SELECT external_execution_id FROM job_runs WHERE id=$1`, runID).Scan(&executionID); err != nil {
		t.Fatal(err)
	}
	if executionID != runID {
		t.Fatalf("external execution ID = %q, want existing run ID %q", executionID, runID)
	}
}
