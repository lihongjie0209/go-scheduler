//go:build integration

package migrations

import (
	"context"
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
}
