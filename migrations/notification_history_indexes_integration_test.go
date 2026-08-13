//go:build integration

package migrations

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestNotificationHistoryIndexesMigration(t *testing.T) {
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
	var definition string
	if err = conn.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='notification_deliveries_channel_history_idx'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	want := "(channel_id, created_at DESC, id DESC)"
	if len(definition) < len(want) || definition[len(definition)-len(want):] != want {
		t.Fatalf("unexpected notification history index: %s", definition)
	}
	if err = conn.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='notification_channels_tenant_active_idx'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	var lifecycleColumns int
	if err = conn.QueryRow(t.Context(), `SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='notification_channels' AND column_name=ANY(ARRAY['version','updated_at','deleted_at'])`).Scan(&lifecycleColumns); err != nil {
		t.Fatal(err)
	}
	if lifecycleColumns != 3 {
		t.Fatalf("notification channel lifecycle columns = %d, want 3", lifecycleColumns)
	}
}
