//go:build integration

package rpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestInProcessSchedulerPersistsJobWithoutEtcd(t *testing.T) {
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
	for _, migration := range migrations.All {
		if _, err = conn.Exec(t.Context(), migration.SQL); err != nil {
			t.Fatalf("migration %d: %v", migration.Version, err)
		}
	}
	var tenantID string
	if err = conn.QueryRow(t.Context(), `INSERT INTO tenants(name) VALUES('standalone') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	if err = conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := store.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	inProcess, err := rpc.NewInProcessScheduler(core.NewService(s), "internal-token", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := inProcess.Close(ctx); closeErr != nil {
			t.Errorf("close in-process scheduler: %v", closeErr)
		}
	})
	created, err := inProcess.Client().CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{
		TenantId: tenantID, Name: "standalone-job", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC",
		TargetUrl: "https://example.com/jobs", HttpMethod: "POST", TimeoutSeconds: 30, OverlapPolicy: "serial",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 3600, MaxQueueSize: 1000,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created.Id == "" || created.TenantId != tenantID {
		t.Fatalf("created job = %+v", created)
	}
}
