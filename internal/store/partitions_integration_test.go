//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestApplicationMaintainsPartitionsOnPlainPostgreSQL(t *testing.T) {
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
	if _, err = conn.Exec(t.Context(), migrations.Up); err != nil {
		t.Fatal(err)
	}
	if err = conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	future := time.Now().UTC().AddDate(0, 6, 0)
	result, err := s.MaintainRunPartitions(t.Context(), future, 90*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if result.Backend != "application" {
		t.Fatalf("backend = %q, want application", result.Backend)
	}
	name := requiredRunPartitions(future, 90*24*time.Hour, 3)[len(requiredRunPartitions(future, 90*24*time.Hour, 3))-1].Name
	var exists bool
	if err = s.pool.QueryRow(t.Context(), `SELECT to_regclass('public.' || $1) IS NOT NULL`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("future partition %s was not created", name)
	}
}
