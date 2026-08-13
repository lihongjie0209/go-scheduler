//go:build integration

package migrations

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestInitialMigrationWorksWithoutPGPartman(t *testing.T) {
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
	if _, err = conn.Exec(t.Context(), Up); err != nil {
		t.Fatalf("initial migration on plain PostgreSQL: %v", err)
	}
	var extension bool
	if err = conn.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_partman')`).Scan(&extension); err != nil {
		t.Fatal(err)
	}
	if extension {
		t.Fatal("plain PostgreSQL unexpectedly has pg_partman")
	}
	var partitions int
	if err = conn.QueryRow(t.Context(), `SELECT count(*) FROM pg_inherits WHERE inhparent='job_runs'::regclass`).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if partitions != 8 {
		t.Fatalf("partitions = %d, want 7 monthly plus default", partitions)
	}
}
