//go:build integration

package migrations

import (
	"context"
	"strings"
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
	if err = conn.QueryRow(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='notification_deliveries_pending_created_idx'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	want = "(created_at) WHERE (status = 'pending'::text)"
	if len(definition) < len(want) || definition[len(definition)-len(want):] != want {
		t.Fatalf("unexpected notification queue metrics index: %s", definition)
	}
	var tenantID, channelID, eventID string
	if err = conn.QueryRow(t.Context(), `INSERT INTO tenants(name) VALUES('metrics-index') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO notification_channels(tenant_id,kind,name,config) VALUES($1,'webhook','metrics-index','{}') RETURNING id`, tenantID).Scan(&channelID); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `INSERT INTO outbox_events(tenant_id,topic,payload) VALUES($1,'job.run.failed','{}') RETURNING id`, tenantID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `INSERT INTO notification_deliveries(event_id,channel_id) VALUES($1,$2)`, eventID, channelID); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(t.Context(), `SET enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err = conn.QueryRow(t.Context(), `EXPLAIN (FORMAT JSON) SELECT count(*),COALESCE(GREATEST(EXTRACT(EPOCH FROM now()-min(created_at)),0),0) FROM notification_deliveries WHERE status='pending'`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "notification_deliveries_pending_created_idx") {
		t.Fatalf("notification queue stats plan does not use covering index: %s", plan)
	}
	var leaseTokenType string
	if err = conn.QueryRow(t.Context(), `SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='job_runs' AND column_name='lease_token'`).Scan(&leaseTokenType); err != nil {
		t.Fatal(err)
	}
	if leaseTokenType != "uuid" {
		t.Fatalf("job run lease token type = %q, want uuid", leaseTokenType)
	}
}
