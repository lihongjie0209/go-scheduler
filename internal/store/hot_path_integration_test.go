//go:build integration

package store

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestListEnqueueAndExpireCallbackHotPaths(t *testing.T) {
	s := newHotPathTestStore(t)
	ctx := t.Context()
	tenantID, err := s.CreateTenant(ctx, "hot-path")
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.CreateJob(ctx, Job{
		TenantID: tenantID, Name: "labeled-a", ScheduleType: "fixed_rate", ScheduleExpression: "60",
		Timezone: "UTC", TargetURL: "https://example.com/a", HTTPMethod: "POST",
		Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false,
		RequiredExecutorLabels: []string{"linux"}, ExcludedExecutorLabels: []string{"gpu"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateJob(ctx, Job{
		TenantID: tenantID, Name: "labeled-b", ScheduleType: "fixed_rate", ScheduleExpression: "60",
		Timezone: "UTC", TargetURL: "https://example.com/b", HTTPMethod: "POST",
		Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false,
		RequiredExecutorLabels: []string{"windows"},
	})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListJobs(ctx, tenantID, 50)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]Job{}
	for _, job := range listed {
		byID[job.ID] = job
	}
	if strings.Join(byID[first.ID].RequiredExecutorLabels, ",") != "linux" || strings.Join(byID[first.ID].ExcludedExecutorLabels, ",") != "gpu" {
		t.Fatalf("first labels = %+v", byID[first.ID])
	}
	if strings.Join(byID[second.ID].RequiredExecutorLabels, ",") != "windows" || len(byID[second.ID].ExcludedExecutorLabels) != 0 {
		t.Fatalf("second labels = %+v", byID[second.ID])
	}

	group, err := s.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "script-workers", RouteStrategy: "first"})
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Repeat("echo ok\n", 8000)
	scriptJob, err := s.CreateJob(ctx, Job{
		TenantID: tenantID, Name: "script-due", ScheduleType: "fixed_rate", ScheduleExpression: "1",
		Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10,
		OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10,
		Enabled: true, ScriptLanguage: "shell", ScriptSource: source, ExecutorHandler: "__script__",
		ExecutorGroupID: group.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, scriptJob.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.EnqueueDue(ctx, 10); err != nil {
		t.Fatal(err)
	}
	runs, err := s.ListRuns(ctx, tenantID, scriptJob.ID, 10)
	if err != nil || len(runs) == 0 {
		t.Fatalf("enqueue due runs = %+v, %v", runs, err)
	}

	expireJob, err := s.CreateJob(ctx, Job{
		TenantID: tenantID, Name: "expire-batch", ScheduleType: "fixed_rate", ScheduleExpression: "60",
		Timezone: "UTC", TargetURL: "https://example.com/expire", HTTPMethod: "POST",
		Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: callbackExpiryBatchSize + 10,
		MaxRetries: 0, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < callbackExpiryBatchSize+1; i++ {
		if _, err = s.TriggerJob(ctx, tenantID, expireJob.ID, fmt.Sprintf("expire-%d", i), ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.pool.Exec(ctx, `UPDATE job_runs SET status='waiting_callback',callback_deadline=now()-interval '1 second' WHERE job_id=$1`, expireJob.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.ExpireCallbacks(ctx); err != nil {
		t.Fatal(err)
	}
	var waiting, timedOut int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='waiting_callback'),count(*) FILTER (WHERE status='timed_out') FROM job_runs WHERE job_id=$1`, expireJob.ID).Scan(&waiting, &timedOut); err != nil {
		t.Fatal(err)
	}
	if waiting != 1 || timedOut != callbackExpiryBatchSize {
		t.Fatalf("expire batch waiting=%d timed_out=%d want waiting=1 timed_out=%d", waiting, timedOut, callbackExpiryBatchSize)
	}
}

func newHotPathTestStore(t *testing.T) *Store {
	t.Helper()
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
	if err = conn.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
