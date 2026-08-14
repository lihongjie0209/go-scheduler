//go:build integration

package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestKubernetesClusterCapacityAcrossConcurrentCoreClaims(t *testing.T) {
	ctx := t.Context()
	projectRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	container, err := postgres.Run(ctx, "", testcontainers.WithDockerfile(testcontainers.FromDockerfile{Context: projectRoot, Dockerfile: "deploy/postgres/Dockerfile", Repo: "go-scheduler-postgres-test", Tag: "16-partman-5.5.0", KeepImage: true}), postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations.All {
		if _, err = connection.Exec(ctx, migration.SQL); err != nil {
			t.Fatalf("migration %d: %v", migration.Version, err)
		}
	}
	var tenantID string
	if err = connection.QueryRow(ctx, `INSERT INTO tenants(name,max_concurrent_runs) VALUES('kubernetes-capacity',1000) RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	_ = connection.Close(ctx)
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	ring, err := cryptox.NewKeyring(1, key)
	if err != nil {
		t.Fatal(err)
	}
	first, err := New(ctx, dsn, WithHeaderCipher(ring))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(ctx, dsn, WithHeaderCipher(ring))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	group, err := first.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "kubernetes-capacity", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := first.CreateKubernetesCluster(ctx, KubernetesCluster{TenantID: tenantID, Name: "limited", AuthMode: "service_account", APIServer: "https://k8s.example", Namespace: "jobs", Credentials: KubernetesCredentials{Token: "token"}, MaxConcurrentJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	job, err := first.CreateJob(ctx, Job{TenantID: tenantID, Name: "kubernetes", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: http.MethodPost, Headers: map[string]string{}, TimeoutSeconds: 60, CallbackTimeoutSeconds: 60, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 100, MaxCatchUp: 10, MaxQueueSize: 100, ExecutorGroupID: group.ID, ExecutorHandler: "__kubernetes__", ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22"}`, KubernetesClusterID: cluster.ID, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	for index := range 8 {
		if _, err = first.TriggerJob(ctx, tenantID, job.ID, fmt.Sprintf("capacity-%d", index), ""); err != nil {
			t.Fatal(err)
		}
	}
	type claimResult struct {
		claims []ClaimedRun
		err    error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for index, schedulerStore := range []*Store{first, second} {
		go func(index int, schedulerStore *Store) {
			<-start
			claims, claimErr := schedulerStore.ClaimRuns(ctx, fmt.Sprintf("core-%d", index), 8, time.Minute)
			results <- claimResult{claims: claims, err: claimErr}
		}(index, schedulerStore)
	}
	close(start)
	claimed := make([]ClaimedRun, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, claim := range result.claims {
			if claim.Job.ID == job.ID {
				claimed = append(claimed, claim)
			}
		}
	}
	if len(claimed) != 2 {
		t.Fatalf("concurrent Core claims = %d, want cluster capacity 2", len(claimed))
	}
	if err = first.CompleteRun(ctx, claimed[0].Run, true, http.StatusOK, "", ""); err != nil {
		t.Fatal(err)
	}
	next, err := second.ClaimRuns(ctx, "core-next", 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	nextKubernetes := 0
	for _, claim := range next {
		if claim.Job.ID == job.ID {
			nextKubernetes++
		}
	}
	if nextKubernetes != 1 {
		t.Fatalf("claims after one slot release = %d, want 1", nextKubernetes)
	}
}
