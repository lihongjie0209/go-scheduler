//go:build integration

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	waitpkg "github.com/testcontainers/testcontainers-go/wait"
)

func TestRegistrarPersistsHeartbeatInPostgreSQL(t *testing.T) {
	ctx := t.Context()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	container, err := postgres.Run(ctx, "", testcontainers.WithDockerfile(testcontainers.FromDockerfile{Context: root, Dockerfile: "deploy/postgres/Dockerfile", Repo: "go-scheduler-postgres-test", Tag: "16-partman-5.5.0", KeepImage: true}), postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
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
	for _, migration := range migrations.All {
		if _, err = conn.Exec(ctx, migration.SQL); err != nil {
			t.Fatalf("migration %d: %v", migration.Version, err)
		}
	}
	var tenantID string
	if err = conn.QueryRow(ctx, `INSERT INTO tenants(name) VALUES('executor-sdk') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	database, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	group, err := database.CreateExecutorGroup(ctx, store.ExecutorGroup{TenantID: tenantID, Name: "sdk", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if err := database.UnregisterExecutorNode(r.Context(), tenantID, group.ID, "sdk-node"); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var body struct {
			Address string `json:"address"`
			TTL     int32  `json:"ttl_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad", 400)
			return
		}
		if _, err := database.RegisterExecutorNode(r.Context(), tenantID, group.ID, "sdk-node", body.Address, time.Duration(body.TTL)*time.Second); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()
	registrar, err := NewRegistrar(RegistrarOptions{APIURL: api.URL, Token: "test", GroupID: group.ID, NodeID: "sdk-node", Address: "http://executor:9999", TTL: 6 * time.Second, HTTPClient: api.Client()})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- registrar.Run(runCtx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		nodes, listErr := database.ListExecutorNodes(ctx, tenantID, group.ID, true)
		if listErr == nil && len(nodes) == 1 && nodes[0].Address == "http://executor:9999" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat not persisted: %v %v", nodes, listErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(fmt.Errorf("registrar: %w", err))
	}
	nodes, err := database.ListExecutorNodes(ctx, tenantID, group.ID, true)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("SDK node remained after shutdown: %+v %v", nodes, err)
	}
}

func TestScriptExecutorImageContainsSupportedRuntimes(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: root, Dockerfile: "deploy/script-executor/Dockerfile", Repo: "go-scheduler-script-executor-test", Tag: "supported-runtimes", KeepImage: true},
		Entrypoint:     []string{"/bin/sh", "-c"},
		Cmd:            []string{`node -e 'process.stdout.write("node:" + process.env.SCHEDULER_INPUT)' && php -r 'fwrite(STDOUT, "|php:" . getenv("SCHEDULER_INPUT"));' && pwsh -NoLogo -NoProfile -NonInteractive -Command '[Console]::Out.Write("|pwsh:" + $env:SCHEDULER_INPUT)'`},
		Env:            map[string]string{"SCHEDULER_INPUT": "container"},
		WaitingFor:     waitpkg.ForLog("node:container|php:container|pwsh:container"),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	reader, err := container.Logs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	buffer := new(strings.Builder)
	if _, err = io.Copy(buffer, reader); err != nil || !strings.Contains(buffer.String(), "node:container|php:container|pwsh:container") {
		t.Fatalf("script image output=%q err=%v", buffer.String(), err)
	}
}

func TestDockerHandlerRunsAndRemovesContainer(t *testing.T) {
	logger := &recordingLogger{}
	handler := DockerHandler(DockerOptions{})
	err := handler(t.Context(), Task{
		RunID: "docker-integration", JobID: "job-docker", Input: "payload", Logger: logger,
		ScriptSource: `{"image":"alpine:3.22","command":["sh","-c"],"args":["printf 'docker:%s' \"$SCHEDULER_INPUT\""],"pull_policy":"always"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(logger.stdout, ""), "docker:payload") {
		t.Fatalf("stdout = %#v", logger.stdout)
	}
}

func TestDockerHandlerResumesManagedContainerAfterExecutorRestart(t *testing.T) {
	const name = "go-scheduler-docker-resume"
	managed, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "alpine:3.22", Name: name,
		Labels: map[string]string{
			"go-scheduler.managed-by": "lihongjie0209",
			"go-scheduler.run-id":     "docker-resume",
			"go-scheduler.job-id":     "job-docker-resume",
		},
		Cmd: []string{"sh", "-c", "sleep 1; printf resumed-output"},
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = managed.Terminate(context.Background()) })
	logger := &recordingLogger{}
	handler := DockerHandler(DockerOptions{})
	err = handler(t.Context(), Task{
		RunID: "docker-resume", JobID: "job-docker-resume", Logger: logger,
		ScriptSource: `{"image":"alpine:3.22","pull_policy":"never"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(logger.stdout, ""), "resumed-output") {
		t.Fatalf("resumed stdout = %#v", logger.stdout)
	}
	if _, err = managed.Inspect(t.Context()); err == nil {
		t.Fatal("resumed container was not removed")
	}
}
