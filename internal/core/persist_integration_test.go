//go:build integration

package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestTerminalWritesPersistAfterParentCancel(t *testing.T) {
	s, tenantID := newPersistTestStore(t)
	ctx := t.Context()
	job, err := s.CreateJob(ctx, store.Job{
		TenantID: tenantID, Name: "persist-fail", ScheduleType: "fixed_rate", ScheduleExpression: "60",
		Timezone: "UTC", TargetURL: "https://example.com/hook", HTTPMethod: "POST",
		Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 4, MaxQueueSize: 20, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	failRun, err := s.TriggerJob(ctx, tenantID, job.ID, "persist-fail", "payload")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimRuns(ctx, "persist-core", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims=%+v err=%v", claims, err)
	}
	parent, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.FailRun(parent, claims[0].Run, "failed", 0, "should not write", nil); err == nil {
		t.Fatal("FailRun with a canceled parent unexpectedly succeeded")
	}
	stillRunning, err := s.GetRun(ctx, tenantID, failRun.ID)
	if err != nil || stillRunning.Status != "running" {
		t.Fatalf("run after canceled FailRun = %+v, %v", stillRunning, err)
	}
	engine := NewEngine(s, "persist-core", time.Second, 1, "http://127.0.0.1", 24*time.Hour, nil)
	engine.fail(parent, claims[0], errors.New("dispatch failed"))
	failed, err := s.GetRun(ctx, tenantID, failRun.ID)
	if err != nil || failed.Status != "failed" || failed.ErrorMessage != "dispatch failed" {
		t.Fatalf("fail after cancel = %+v, %v", failed, err)
	}

	callbackRun, err := s.TriggerJob(ctx, tenantID, job.ID, "persist-callback", "callback")
	if err != nil {
		t.Fatal(err)
	}
	callbackClaims, err := s.ClaimRuns(ctx, "persist-core", 10, time.Minute)
	if err != nil || len(callbackClaims) != 1 {
		t.Fatalf("callback claims=%+v err=%v", callbackClaims, err)
	}
	tokenHash := sha256.Sum256([]byte("persist-callback-token"))
	if err = s.MarkClaimedWaitingCallback(ctx, callbackClaims[0].Run, 202, tokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = persistWrite(parent, func(ackCtx context.Context) error {
		return s.CompleteCallback(ackCtx, callbackRun.ID, tokenHash[:], true, "")
	}); err != nil {
		t.Fatalf("complete callback after cancel: %v", err)
	}
	completed, err := s.GetRun(ctx, tenantID, callbackRun.ID)
	if err != nil || completed.Status != "succeeded" {
		t.Fatalf("callback after cancel = %+v, %v", completed, err)
	}

	commandRun, err := s.TriggerJob(ctx, tenantID, job.ID, "persist-command", "command")
	if err != nil {
		t.Fatal(err)
	}
	commandClaims, err := s.ClaimRuns(ctx, "persist-core", 10, time.Minute)
	if err != nil || len(commandClaims) != 1 {
		t.Fatalf("command claims=%+v err=%v", commandClaims, err)
	}
	commandHash := sha256.Sum256([]byte("persist-command-token"))
	if err = s.PrepareClaimedExecutorDispatch(ctx, commandClaims[0].Run, "node-1", "127.0.0.1:19091", commandHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CancelRun(ctx, tenantID, commandRun.ID, "stop"); err != nil {
		t.Fatal(err)
	}
	commands, err := s.ClaimExecutorCommands(ctx, "persist-core", 1)
	if err != nil || len(commands) != 1 {
		t.Fatalf("commands=%+v err=%v", commands, err)
	}
	if err = persistWrite(parent, func(ackCtx context.Context) error {
		return s.CompleteExecutorCommand(ackCtx, "persist-core", commands[0].ID)
	}); err != nil {
		t.Fatalf("complete command after cancel: %v", err)
	}
	stats, err := s.ExecutorCommandQueueStats(ctx)
	if err != nil || stats.Pending != 0 {
		t.Fatalf("command queue after cancel ack = %+v, %v", stats, err)
	}
}

func newPersistTestStore(t *testing.T) (*store.Store, string) {
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
	s, err := store.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	tenantID, err := s.CreateTenant(t.Context(), "persist")
	if err != nil {
		t.Fatal(err)
	}
	return s, tenantID
}
