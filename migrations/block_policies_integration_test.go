//go:build integration

package migrations

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestBlockPolicyMigrationUpgradesExistingJobs(t *testing.T) {
	ctx := t.Context()
	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err = conn.Exec(ctx, `CREATE TABLE jobs(id int PRIMARY KEY, overlap_policy text NOT NULL CHECK (overlap_policy IN ('skip','queue','parallel'))); INSERT INTO jobs VALUES(1,'skip'),(2,'queue'),(3,'parallel')`); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, BlockPoliciesUp); err != nil {
		t.Fatalf("upgrade migration failed: %v", err)
	}
	rows, err := conn.Query(ctx, `SELECT overlap_policy FROM jobs ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var policy string
		if err = rows.Scan(&policy); err != nil {
			t.Fatal(err)
		}
		got = append(got, policy)
	}
	want := []string{"discard_later", "serial", "parallel"}
	if len(got) != len(want) {
		t.Fatalf("policies = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("policies = %v, want %v", got, want)
		}
	}
}
