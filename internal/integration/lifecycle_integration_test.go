//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/notifier"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"github.com/lihongjie0209/go-scheduler/migrations"
	executorsdk "github.com/lihongjie0209/go-scheduler/pkg/executor"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	waitpkg "github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifecycleFixture struct {
	store    *store.Store
	service  *core.Service
	client   schedulerv1.SchedulerServiceClient
	tenantID string
	dsn      string
	cipher   store.HeaderCipher
	close    func()
}

type haCoreNode struct {
	store *store.Store
	calls atomic.Int32
	stop  func()
}

type cancellationRecordingExecutor struct {
	executorv1.UnimplementedExecutorServiceServer
	requests chan *executorv1.CancelRequest
}

func (e *cancellationRecordingExecutor) Cancel(_ context.Context, request *executorv1.CancelRequest) (*executorv1.CancelResponse, error) {
	e.requests <- request
	return &executorv1.CancelResponse{Accepted: true}, nil
}

func newEtcdFixture(t *testing.T) *clientv3.Client {
	t.Helper()
	ctx := t.Context()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "quay.io/coreos/etcd:v3.5.17", ExposedPorts: []string{"2379/tcp"}, Cmd: []string{"/usr/local/bin/etcd", "--advertise-client-urls=http://0.0.0.0:2379", "--listen-client-urls=http://0.0.0.0:2379"}, WaitingFor: waitpkg.ForListeningPort("2379/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "2379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	client, err := discovery.NewClient([]string{net.JoinHostPort(host, port.Port())}, "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startHACore(t *testing.T, etcd *clientv3.Client, dsn, prefix, instanceID string, cipher store.HeaderCipher) *haCoreNode {
	t.Helper()
	database, err := store.New(t.Context(), dsn, store.WithHeaderCipher(cipher))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &haCoreNode{store: database}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		node.calls.Add(1)
		return handler(ctx, req)
	}))
	schedulerv1.RegisterSchedulerServiceServer(server, core.NewService(database))
	go func() { _ = server.Serve(listener) }()
	registrar, err := discovery.NewRegistrar(etcd, prefix, "scheduler-core", discovery.Metadata{InstanceID: instanceID, GRPCAddress: listener.Addr().String(), StartedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- registrar.Run(runCtx) }()
	deadline := time.Now().Add(10 * time.Second)
	key := prefix + "/scheduler-core/" + instanceID
	for {
		response, getErr := etcd.Get(t.Context(), key)
		if getErr == nil && len(response.Kvs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("core %s was not registered", instanceID)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var stopped atomic.Bool
	node.stop = func() {
		if !stopped.CompareAndSwap(false, true) {
			return
		}
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = registrar.Close(closeCtx)
		closeCancel()
		server.Stop()
		_ = listener.Close()
		database.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}
	t.Cleanup(node.stop)
	return node
}

func newDiscoveredCoreClient(t *testing.T, etcd *clientv3.Client, prefix string) (schedulerv1.SchedulerServiceClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := grpc.NewClient("etcd:///scheduler-core", grpc.WithResolvers(discovery.NewBuilder(etcd, prefix)), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, roundrobin.Name)))
	if err != nil {
		t.Fatal(err)
	}
	return schedulerv1.NewSchedulerServiceClient(conn), conn
}

func newLifecycleFixture(t *testing.T) lifecycleFixture {
	return newLifecycleFixtureWithEncryption(t)
}

func newEncryptedLifecycleFixture(t *testing.T) lifecycleFixture {
	return newLifecycleFixtureWithEncryption(t)
}

func newLifecycleFixtureWithEncryption(t *testing.T) lifecycleFixture {
	t.Helper()
	ctx := t.Context()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	container, err := postgres.Run(ctx, "", testcontainers.WithDockerfile(testcontainers.FromDockerfile{Context: root, Dockerfile: "deploy/postgres/Dockerfile", Repo: "go-scheduler-postgres-test", Tag: "16-partman-5.5.0", KeepImage: true}), postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
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
	if err = conn.QueryRow(ctx, `INSERT INTO tenants(name) VALUES('lifecycle') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(ctx)
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	ring, ringErr := cryptox.NewKeyring(1, key)
	if ringErr != nil {
		t.Fatal(ringErr)
	}
	options := []store.Option{store.WithHeaderCipher(ring)}
	database, err := store.New(ctx, dsn, options...)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	service := core.NewService(database)
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	grpcConn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return lifecycleFixture{store: database, service: service, client: schedulerv1.NewSchedulerServiceClient(grpcConn), tenantID: tenantID, dsn: dsn, cipher: ring, close: func() {
		_ = grpcConn.Close()
		grpcServer.Stop()
		database.Close()
		_ = container.Terminate(context.Background())
	}}
}

func createDisabledJob(t *testing.T, fixture lifecycleFixture) store.Job {
	t.Helper()
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "lifecycle", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: "skip", MisfirePolicy: "fire_once", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func createHTTPExecutorGroup(t *testing.T, fixture lifecycleFixture) *schedulerv1.ExecutorGroup {
	t.Helper()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "http-" + strings.ReplaceAll(t.Name(), "/", "-"), RouteStrategy: "round"}})
	if err != nil {
		t.Fatal(err)
	}
	return group
}

func startTestGRPCExecutor(t *testing.T, fixture lifecycleFixture, handlers map[string]executorsdk.Handler) string {
	t.Helper()
	sdk, err := executorsdk.NewServer(executorsdk.Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	for name, handler := range handlers {
		if err = sdk.Handle(name, handler); err != nil {
			t.Fatal(err)
		}
	}
	reporter, err := executorsdk.NewGRPCReporter(fixture.client)
	if err != nil {
		t.Fatal(err)
	}
	service, err := executorsdk.NewGRPCServer(sdk, reporter)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(server, service)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return "grpc://" + listener.Addr().String()
}

func (f lifecycleFixture) useEngine(engine *core.Engine) {
	if f.service != nil {
		f.service.SetOnRunTerminal(engine.WakeDispatch)
	}
}

func attachHTTPExecutor(t *testing.T, fixture lifecycleFixture) string {
	t.Helper()
	group := createHTTPExecutorGroup(t, fixture)
	address := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"__http__": executorsdk.HTTPHandler(nil)})
	if _, err := fixture.store.RegisterExecutorNode(t.Context(), fixture.tenantID, group.Id, "http-node", address, time.Minute); err != nil {
		t.Fatal(err)
	}
	return group.Id
}

type routingProbeExecutor struct {
	executorv1.UnimplementedExecutorServiceServer
	reporter executorsdk.Reporter
	calls    *atomic.Int32
	busy     bool
}

func (p *routingProbeExecutor) Dispatch(_ context.Context, req *executorv1.DispatchRequest) (*executorv1.DispatchResponse, error) {
	p.calls.Add(1)
	go func() {
		_ = p.reporter.Complete(context.Background(), req.GetRunId(), req.GetCallbackToken(), true, "")
	}()
	return &executorv1.DispatchResponse{Accepted: true, ExecutionId: req.GetRunId(), State: "running"}, nil
}

func (p *routingProbeExecutor) Inspect(context.Context, *executorv1.InspectRequest) (*executorv1.ExecutionState, error) {
	state := "idle"
	if p.busy {
		state = "busy"
	}
	return &executorv1.ExecutionState{State: state}, nil
}

func (p *routingProbeExecutor) Cancel(context.Context, *executorv1.CancelRequest) (*executorv1.CancelResponse, error) {
	return &executorv1.CancelResponse{Accepted: true}, nil
}

func startProbeLifecycleExecutor(t *testing.T, fixture lifecycleFixture, serving, busy bool, calls *atomic.Int32) string {
	t.Helper()
	reporter, err := executorsdk.NewGRPCReporter(fixture.client)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(server, &routingProbeExecutor{reporter: reporter, calls: calls, busy: busy})
	healthServer := health.NewServer()
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthpb.HealthCheckResponse_SERVING
	}
	healthServer.SetServingStatus("", status)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return "grpc://" + listener.Addr().String()
}

func countingHandler(counter *atomic.Int32) executorsdk.Handler {
	return func(context.Context, executorsdk.Task) error {
		counter.Add(1)
		return nil
	}
}

func createPolicyJob(t *testing.T, fixture lifecycleFixture, name, policy string) store.Job {
	t.Helper()
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: name, ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: policy, MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestCrossModuleJobLifecycleThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createDisabledJob(t, fixture)

	started, err := fixture.client.SetJobEnabled(t.Context(), &schedulerv1.SetJobEnabledRequest{TenantId: fixture.tenantID, Id: job.ID, Enabled: true, Version: job.Version})
	if err != nil {
		t.Fatal(err)
	}
	if !started.Enabled || started.NextRunAt == nil || started.Version != job.Version+1 {
		t.Fatalf("invalid started job: %+v", started)
	}
}

func TestCrossModuleEtcdCoreFailoverThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	etcd := newEtcdFixture(t)
	prefix := "/ha-cross/services"
	first := startHACore(t, etcd, fixture.dsn, prefix, "core-1", fixture.cipher)
	second := startHACore(t, etcd, fixture.dsn, prefix, "core-2", fixture.cipher)
	client, conn := newDiscoveredCoreClient(t, etcd, prefix)
	defer conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	for (first.calls.Load() == 0 || second.calls.Load() == 0) && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, _ = client.ListJobs(ctx, &schedulerv1.ListJobsRequest{TenantId: fixture.tenantID, Limit: 10})
		cancel()
		time.Sleep(20 * time.Millisecond)
	}
	if first.calls.Load() == 0 || second.calls.Load() == 0 {
		t.Fatalf("round robin calls: first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	first.stop()
	deadline = time.Now().Add(10 * time.Second)
	for {
		response, err := etcd.Get(t.Context(), prefix+"/scheduler-core/", clientv3.WithPrefix())
		if err == nil && len(response.Kvs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stopped core remained registered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	before := second.calls.Load()
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, err := client.ListJobs(ctx, &schedulerv1.ListJobsRequest{TenantId: fixture.tenantID, Limit: 10})
		cancel()
		if err != nil {
			t.Fatalf("request after failover: %v", err)
		}
	}
	if second.calls.Load() < before+5 {
		t.Fatalf("remaining core calls=%d, before=%d", second.calls.Load(), before)
	}
}

func TestCrossModuleRunReportThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createDisabledJob(t, fixture)
	for index, statusName := range []string{"succeeded", "failed", "timed_out", "cancelled", "skipped", "running"} {
		run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, fmt.Sprintf("report-%d", index), "")
		if err != nil {
			t.Fatal(err)
		}
		conn, connectErr := pgx.Connect(t.Context(), fixture.dsn)
		if connectErr != nil {
			t.Fatal(connectErr)
		}
		_, err = conn.Exec(t.Context(), `UPDATE job_runs SET status=$2 WHERE id=$1`, run.ID, statusName)
		_ = conn.Close(t.Context())
		if err != nil {
			t.Fatal(err)
		}
	}
	today := time.Now().UTC().Format(time.DateOnly)
	report, err := fixture.client.GetRunReport(t.Context(), &schedulerv1.GetRunReportRequest{TenantId: fixture.tenantID, FromDate: today, ToDate: today, Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Points) != 1 || report.Points[0].Total != 6 || report.Points[0].Succeeded != 1 || report.Points[0].Failed != 2 || report.Points[0].Active != 1 || report.Points[0].Cancelled != 1 || report.Points[0].Skipped != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCrossModuleRunHistoryPurgeThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "purge-history", "parallel")
	terminal, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "purge-terminal", "")
	if err != nil {
		t.Fatal(err)
	}
	active, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "purge-active", "")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(t.Context(), `UPDATE job_runs SET status='succeeded', scheduled_at=now()-interval '2 hours' WHERE id=$1`, terminal.ID)
	_ = conn.Close(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	response, err := fixture.client.PurgeRunHistory(t.Context(), &schedulerv1.PurgeRunHistoryRequest{TenantId: fixture.tenantID, JobId: job.ID, Before: timestamppb.New(time.Now()), Limit: 10})
	if err != nil || response.Deleted != 1 {
		t.Fatalf("purge=%+v err=%v", response, err)
	}
	if _, err = fixture.client.GetRun(t.Context(), &schedulerv1.GetRunRequest{TenantId: fixture.tenantID, RunId: terminal.ID}); status.Code(err) != codes.NotFound {
		t.Fatalf("terminal get=%v", err)
	}
	if _, err = fixture.client.GetRun(t.Context(), &schedulerv1.GetRunRequest{TenantId: fixture.tenantID, RunId: active.ID}); err != nil {
		t.Fatalf("active run removed: %v", err)
	}
	repeated, err := fixture.client.PurgeRunHistory(t.Context(), &schedulerv1.PurgeRunHistoryRequest{TenantId: fixture.tenantID, Before: timestamppb.New(time.Now()), Limit: 10})
	if err != nil || repeated.Deleted != 0 {
		t.Fatalf("repeated purge=%+v err=%v", repeated, err)
	}
}

func TestCrossModuleJobCRUDThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()

	group := createHTTPExecutorGroup(t, fixture)
	created, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{
		TenantId: fixture.tenantID, Name: "grpc-crud", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC",
		TargetUrl: "https://example.com/jobs", HttpMethod: "POST", TimeoutSeconds: 30, OverlapPolicy: "serial",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 3600, MaxQueueSize: 1000,
		ExecutorGroupId: group.Id, ExecutorHandler: "__http__",
	}})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := fixture.client.ListJobs(t.Context(), &schedulerv1.ListJobsRequest{TenantId: fixture.tenantID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Jobs) != 1 || listed.Jobs[0].Id != created.Id {
		t.Fatalf("listed jobs = %+v", listed.Jobs)
	}

	staleVersion := created.Version
	created.Name = "grpc-crud-updated"
	created.Description = "updated over gRPC"
	updated, err := fixture.client.UpdateJob(t.Context(), &schedulerv1.UpdateJobRequest{Job: created})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != staleVersion+1 || updated.Name != "grpc-crud-updated" {
		t.Fatalf("updated job = %+v", updated)
	}
	created.Version = staleVersion
	if _, err = fixture.client.UpdateJob(t.Context(), &schedulerv1.UpdateJobRequest{Job: created}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale update code = %v, want Aborted: %v", status.Code(err), err)
	}
	if _, err = fixture.client.DeleteJob(t.Context(), &schedulerv1.DeleteJobRequest{TenantId: fixture.tenantID, Id: updated.Id, Version: updated.Version}); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: updated.Id}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted get code = %v, want NotFound: %v", status.Code(err), err)
	}
}

func TestCrossModuleCronSchedulingThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group := createHTTPExecutorGroup(t, fixture)
	created, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{
		TenantId: fixture.tenantID, Name: "grpc-cron", ScheduleType: "cron", ScheduleExpression: "0/1 * * * * ?", Timezone: "Asia/Shanghai",
		TargetUrl: "https://example.com/cron", HttpMethod: "POST", TimeoutSeconds: 30, OverlapPolicy: "parallel",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 3600, MaxQueueSize: 10, Enabled: true,
		ExecutorGroupId: group.Id, ExecutorHandler: "__http__",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if created.NextRunAt == nil || created.Timezone != "Asia/Shanghai" {
		t.Fatalf("created cron job = %+v", created)
	}
	wait := time.Until(created.NextRunAt.AsTime()) + 50*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}
	if err = fixture.store.EnqueueDue(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	runs, err := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, JobId: created.Id, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 1 || runs.Runs[0].TriggerType != "schedule" {
		t.Fatalf("cron runs = %+v", runs.Runs)
	}
	advanced, err := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: created.Id})
	if err != nil {
		t.Fatal(err)
	}
	if advanced.NextRunAt == nil || !advanced.NextRunAt.AsTime().After(runs.Runs[0].ScheduledAt.AsTime()) {
		t.Fatalf("cron next_run_at did not advance: job=%+v run=%+v", advanced, runs.Runs[0])
	}
}

func TestCrossModuleQuartzCalendarSchedulingThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group := createHTTPExecutorGroup(t, fixture)
	created, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "grpc-quartz-last-weekday", ScheduleType: "cron", ScheduleExpression: "0 0 9 LW * ?", Timezone: "UTC", TargetUrl: "https://example.com/quartz", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, Enabled: true, ExecutorGroupId: group.Id, ExecutorHandler: "__http__"}})
	if err != nil || created.NextRunAt == nil {
		t.Fatalf("created quartz = %+v, %v", created, err)
	}
	next := created.NextRunAt.AsTime().UTC()
	if next.Weekday() == time.Saturday || next.Weekday() == time.Sunday {
		t.Fatalf("LW scheduled on weekend: %s", next)
	}
	if next.AddDate(0, 0, 1).Month() == next.Month() && next.AddDate(0, 0, 2).Month() == next.Month() && next.AddDate(0, 0, 3).Month() == next.Month() {
		t.Fatalf("LW is not within the final three calendar days: %s", next)
	}
	loaded, err := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: created.Id})
	if err != nil || loaded.ScheduleExpression != "0 0 9 LW * ?" || !loaded.NextRunAt.AsTime().Equal(created.NextRunAt.AsTime()) {
		t.Fatalf("loaded quartz = %+v, %v", loaded, err)
	}
	_, err = fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "invalid-quartz", ScheduleType: "cron", ScheduleExpression: "0 0 9 ? * 2#6", Timezone: "UTC", TargetUrl: "https://example.com/quartz", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "__http__"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid Quartz code=%s err=%v", status.Code(err), err)
	}
}

func TestCrossModuleSchedulePreviewThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	response, err := fixture.client.PreviewSchedule(t.Context(), &schedulerv1.PreviewScheduleRequest{ScheduleType: "cron", ScheduleExpression: "0 0 9 ? * 2#1", Timezone: "UTC", After: timestamppb.New(time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)), Count: 3})
	if err != nil || len(response.TriggerTimes) != 3 {
		t.Fatalf("preview = %+v, %v", response, err)
	}
	want := []time.Time{time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC), time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)}
	for index, triggerTime := range response.TriggerTimes {
		if !triggerTime.AsTime().Equal(want[index]) {
			t.Fatalf("trigger[%d]=%s want=%s", index, triggerTime.AsTime(), want[index])
		}
	}
	_, err = fixture.client.PreviewSchedule(t.Context(), &schedulerv1.PreviewScheduleRequest{ScheduleType: "cron", ScheduleExpression: "invalid", Timezone: "UTC"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid preview code=%s err=%v", status.Code(err), err)
	}
}

func TestCrossModuleMisfirePoliciesThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	direct, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close(t.Context())

	jobs := make(map[string]*schedulerv1.Job, 3)
	group := createHTTPExecutorGroup(t, fixture)
	for _, policy := range []string{"skip", "fire_once", "catch_up"} {
		created, createErr := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{
			TenantId: fixture.tenantID, Name: "grpc-misfire-" + policy, ScheduleType: "fixed_rate", ScheduleExpression: "1", Timezone: "UTC",
			TargetUrl: "https://example.com/misfire", HttpMethod: "POST", TimeoutSeconds: 30, OverlapPolicy: "parallel",
			MisfirePolicy: policy, MaxConcurrentRuns: 1, MaxCatchUp: 3, CallbackTimeoutSeconds: 3600, MaxQueueSize: 10, Enabled: true,
			ExecutorGroupId: group.Id, ExecutorHandler: "__http__",
		}})
		if createErr != nil {
			t.Fatal(createErr)
		}
		jobs[policy] = created
		if _, err = direct.Exec(t.Context(), `UPDATE jobs SET next_run_at=date_trunc('second',now())-interval '10 seconds' WHERE id=$1`, created.Id); err != nil {
			t.Fatal(err)
		}
	}
	if err = fixture.store.EnqueueDue(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	for policy, want := range map[string]int{"skip": 0, "fire_once": 1, "catch_up": 3} {
		runs, listErr := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, JobId: jobs[policy].Id, Limit: 10})
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(runs.Runs) != want {
			t.Fatalf("misfire %s runs=%+v, want %d", policy, runs.Runs, want)
		}
		advanced, getErr := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: jobs[policy].Id})
		if getErr != nil {
			t.Fatal(getErr)
		}
		if advanced.NextRunAt == nil || !advanced.NextRunAt.AsTime().After(time.Now().Add(-time.Second)) {
			t.Fatalf("misfire %s did not advance: %+v", policy, advanced)
		}
	}
}

func TestCrossModuleManualTriggerIdempotencyThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createDisabledJob(t, fixture)
	type triggerResult struct {
		run *schedulerv1.Run
		err error
	}
	results := make(chan triggerResult, 2)
	start := make(chan struct{})
	for _, input := range []string{"first-input", "second-input"} {
		input := input
		go func() {
			<-start
			run, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "grpc-idempotent", Input: input})
			results <- triggerResult{run: run, err: err}
		}()
	}
	close(start)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("trigger errors = %v, %v", first.err, second.err)
	}
	if first.run.Id != second.run.Id {
		t.Fatalf("idempotent runs differ: %+v, %+v", first.run, second.run)
	}
	listed, err := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, JobId: job.ID, Limit: 10})
	if err != nil || len(listed.Runs) != 1 {
		t.Fatalf("listed runs = %+v, %v", listed.Runs, err)
	}
	_, err = fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: "00000000-0000-0000-0000-000000000099", IdempotencyKey: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing job code = %s, want NotFound: %v", status.Code(err), err)
	}
	_, err = fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: strings.Repeat("k", 201)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid key code = %s, want InvalidArgument: %v", status.Code(err), err)
	}
}

func TestCrossModuleTriggerAddressOverrideThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "override-cross", RouteStrategy: "round"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []struct{ id, address string }{{"worker-a", "http://worker-a:9999"}, {"worker-b", "http://worker-b:9999"}} {
		if _, err = fixture.client.RegisterExecutorNode(t.Context(), &schedulerv1.RegisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: node.id, Address: node.address, TtlSeconds: 30}); err != nil {
			t.Fatal(err)
		}
	}
	job, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "override-cross-job", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "override.handler"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.Id, IdempotencyKey: "override-rejected", OverrideAddresses: []string{"http://evil:9999"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unregistered override code = %s, want InvalidArgument: %v", status.Code(err), err)
	}
	addresses := []string{"http://worker-b:9999/", "http://worker-a:9999", "http://worker-a:9999/"}
	run, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.Id, IdempotencyKey: "override-cross", Input: "payload", OverrideAddresses: addresses})
	if err != nil || strings.Join(run.OverrideAddresses, ",") != "http://worker-a:9999,http://worker-b:9999" {
		t.Fatalf("override run = %+v, %v", run, err)
	}
	repeated, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.Id, IdempotencyKey: "override-cross", Input: "changed", OverrideAddresses: []string{"http://ignored:9999"}})
	if err != nil || repeated.Id != run.Id || strings.Join(repeated.OverrideAddresses, ",") != strings.Join(run.OverrideAddresses, ",") {
		t.Fatalf("idempotent override = %+v, %v", repeated, err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "override-cross-core", 10, time.Minute)
	if err != nil || len(claims) != 1 || strings.Join(claims[0].Run.OverrideAddresses, ",") != strings.Join(run.OverrideAddresses, ",") {
		t.Fatalf("override claim = %+v, %v", claims, err)
	}
}

func TestCrossModuleShardingBroadcastThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "grpc-broadcast", RouteStrategy: "sharding_broadcast"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"node-b", "node-a"} {
		if _, err = fixture.client.RegisterExecutorNode(t.Context(), &schedulerv1.RegisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: nodeID, Address: "http://" + nodeID + ":9999", TtlSeconds: 30}); err != nil {
			t.Fatal(err)
		}
	}
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "grpc-broadcast", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: group.Id, ExecutorHandler: "broadcast", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "grpc-broadcast"})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, BroadcastGroupId: primary.BroadcastGroupId, Limit: 10})
	if err != nil || len(listed.Runs) != 2 || listed.Runs[0].ShardIndex != 0 || listed.Runs[0].ShardTotal != 2 || listed.Runs[1].ShardIndex != 1 {
		t.Fatalf("gRPC broadcast runs = %+v, %v", listed, err)
	}
}

func TestCrossModuleRunLogsThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "grpc-run-logs", "parallel")
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "grpc-log", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "grpc-log-core", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims = %+v, %v", claims, err)
	}
	token := "grpc-log-token"
	hash := sha256.Sum256([]byte(token))
	if err = fixture.store.ActivateClaimedRunToken(t.Context(), claims[0].Run, hash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	appended, err := fixture.client.AppendRunLogs(t.Context(), &schedulerv1.AppendRunLogsRequest{RunId: run.ID, Token: token, Entries: []*schedulerv1.RunLogInput{{EntryId: "grpc-1", Stream: "stdout", Content: "hello grpc"}}})
	if err != nil || appended.NextCursor < 1 {
		t.Fatalf("append = %+v, %v", appended, err)
	}
	listed, err := fixture.client.ListRunLogs(t.Context(), &schedulerv1.ListRunLogsRequest{TenantId: fixture.tenantID, RunId: run.ID, Limit: 10})
	if err != nil || len(listed.Entries) != 1 || listed.Entries[0].Content != "hello grpc" || listed.NextCursor != appended.NextCursor {
		t.Fatalf("listed logs = %+v, %v", listed, err)
	}
}

func TestCrossModuleFixedDelayThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group := createHTTPExecutorGroup(t, fixture)
	created, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "grpc-fixed-delay", ScheduleType: "fixed_delay", ScheduleExpression: "2", Timezone: "UTC", TargetUrl: "https://example.com", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 3600, MaxQueueSize: 10, Enabled: true, ExecutorGroupId: group.Id, ExecutorHandler: "__http__"}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err = fixture.store.EnqueueDue(t.Context(), 10); err != nil {
			t.Fatal(err)
		}
		state, getErr := fixture.store.GetJob(t.Context(), fixture.tenantID, created.Id)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if state.NextRunAt == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixed delay job did not become due")
		}
		time.Sleep(25 * time.Millisecond)
	}
	pending, err := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: created.Id})
	if err != nil || !pending.Enabled || pending.NextRunAt != nil {
		t.Fatalf("pending fixed delay = %+v, %v", pending, err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "grpc-fixed-delay", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims = %+v, %v", claims, err)
	}
	finishedAfter := time.Now().UTC()
	if err = fixture.store.CompleteRun(t.Context(), claims[0].Run, true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	rearmed, err := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: created.Id})
	if err != nil || rearmed.NextRunAt == nil || rearmed.NextRunAt.AsTime().Before(finishedAfter.Add(1900*time.Millisecond)) {
		t.Fatalf("rearmed fixed delay = %+v, %v", rearmed, err)
	}
}

func TestCrossModuleAsyncCallbackRetryThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "grpc-callback-retry", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com/callback", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	triggered, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "grpc-callback", Input: "payload"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "grpc-callback-core", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var claim store.ClaimedRun
	for _, candidate := range claims {
		if candidate.Run.ID == triggered.Id {
			claim = candidate
		}
	}
	if claim.Run.ID == "" {
		t.Fatal("callback run was not claimed")
	}
	token := "grpc-callback-token"
	hash := sha256.Sum256([]byte(token))
	if err = fixture.store.MarkClaimedWaitingCallback(t.Context(), claim.Run, http.StatusAccepted, hash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.client.CompleteCallback(t.Context(), &schedulerv1.CompleteCallbackRequest{RunId: claim.Run.ID, Token: token, Succeeded: false, Message: "async failed"}); err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.client.GetRun(t.Context(), &schedulerv1.GetRunRequest{TenantId: fixture.tenantID, RunId: claim.Run.ID})
	if err != nil || failed.Status != "failed" {
		t.Fatalf("failed callback = %+v, %v", failed, err)
	}
	runs, err := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, JobId: job.ID, Limit: 10})
	if err != nil || len(runs.Runs) != 2 {
		t.Fatalf("callback runs = %+v, %v", runs.Runs, err)
	}
	var retry *schedulerv1.Run
	for _, run := range runs.Runs {
		if run.RetryOfRunId == claim.Run.ID {
			retry = run
		}
	}
	if retry == nil || retry.Attempt != 2 || retry.TriggerType != "retry" {
		t.Fatalf("callback retry = %+v", retry)
	}
	_, err = fixture.client.CompleteCallback(t.Context(), &schedulerv1.CompleteCallbackRequest{RunId: claim.Run.ID, Token: token, Succeeded: true})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("replayed callback code = %s, want NotFound: %v", status.Code(err), err)
	}
}

func TestCrossModuleNotificationChannelsThroughGRPC(t *testing.T) {
	fixture := newEncryptedLifecycleFixture(t)
	defer fixture.close()
	const secretConfig = `{"url":"https://alerts.example.com/hook","headers":{"Authorization":"Bearer notification-secret"}}`
	created, err := fixture.client.CreateNotificationChannel(t.Context(), &schedulerv1.CreateNotificationChannelRequest{TenantId: fixture.tenantID, Kind: "webhook", Name: "grpc-alerts", ConfigJson: []byte(secretConfig)})
	if err != nil || created.Id == "" || created.Kind != "webhook" || !created.Configured || !created.Enabled || created.Version != 1 {
		t.Fatalf("created channel = %+v, %v", created, err)
	}
	updated, err := fixture.client.UpdateNotificationChannel(t.Context(), &schedulerv1.UpdateNotificationChannelRequest{Id: created.Id, TenantId: fixture.tenantID, Kind: "webhook", Name: "grpc-alerts-updated", Events: []string{"failed"}, AllJobs: true, MaxAttempts: 4, BackoffInitialSeconds: 3, BackoffMaxSeconds: 60, Version: created.Version})
	if err != nil || updated.Name != "grpc-alerts-updated" || updated.Version != 2 {
		t.Fatalf("updated channel = %+v, %v", updated, err)
	}
	stored, err := fixture.store.NotificationChannel(t.Context(), fixture.tenantID, created.Id)
	if err != nil || string(stored.Config) != secretConfig {
		t.Fatalf("preserved notification config = %s, %v", stored.Config, err)
	}
	connection, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(t.Context())
	var plaintextConfig, encryptedConfig []byte
	if err = connection.QueryRow(t.Context(), `SELECT config,encrypted_config FROM notification_channels WHERE id=$1`, created.Id).Scan(&plaintextConfig, &encryptedConfig); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plaintextConfig, []byte("notification-secret")) || bytes.Contains(encryptedConfig, []byte("notification-secret")) {
		t.Fatal("notification credential was stored in plaintext")
	}
	disabled, err := fixture.client.SetNotificationChannelEnabled(t.Context(), &schedulerv1.SetNotificationChannelEnabledRequest{Id: created.Id, TenantId: fixture.tenantID, Enabled: false, Version: updated.Version})
	if err != nil || disabled.Enabled || disabled.Version != 3 {
		t.Fatalf("disabled channel = %+v, %v", disabled, err)
	}
	listed, err := fixture.client.ListNotificationChannels(t.Context(), &schedulerv1.ListNotificationChannelsRequest{TenantId: fixture.tenantID})
	if err != nil || len(listed.Channels) != 1 || listed.Channels[0].Id != created.Id || listed.Channels[0].Enabled {
		t.Fatalf("listed channels = %+v, %v", listed, err)
	}
	if _, err = fixture.client.DeleteNotificationChannel(t.Context(), &schedulerv1.DeleteNotificationChannelRequest{Id: created.Id, TenantId: fixture.tenantID, Version: disabled.Version}); err != nil {
		t.Fatal(err)
	}
	listed, err = fixture.client.ListNotificationChannels(t.Context(), &schedulerv1.ListNotificationChannelsRequest{TenantId: fixture.tenantID})
	if err != nil || len(listed.Channels) != 0 {
		t.Fatalf("channels after delete = %+v, %v", listed, err)
	}
}

func TestCrossModuleBlockPoliciesThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "cross-cover", "cover_early")
	first, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "first"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cross-core", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim first run: count=%d err=%v", len(claims), err)
	}
	second, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "second"})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	statuses := map[string]string{}
	for _, run := range runs {
		statuses[run.ID] = run.Status
	}
	if statuses[first.Id] != "cancelled" || statuses[second.Id] != "pending" {
		t.Fatalf("cover statuses = %v", statuses)
	}
}

func TestCrossModuleCancelRunThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "cross-cancel", "serial")
	run, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "cancel"})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := fixture.client.CancelRun(t.Context(), &schedulerv1.CancelRunRequest{TenantId: fixture.tenantID, RunId: run.Id, Reason: "cross-module cancellation"})
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != "cancelled" || cancelled.ErrorMessage != "cross-module cancellation" {
		t.Fatalf("cancelled run = %+v", cancelled)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cross-cancel-core", 10, time.Minute)
	if err != nil || len(claims) != 0 {
		t.Fatalf("cancelled run was claimable: %+v, %v", claims, err)
	}
}

func TestCancelRunStopsAssignedGRPCExecutor(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	executorListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	executorServer := grpc.NewServer()
	recorder := &cancellationRecordingExecutor{requests: make(chan *executorv1.CancelRequest, 1)}
	executorv1.RegisterExecutorServiceServer(executorServer, recorder)
	go func() { _ = executorServer.Serve(executorListener) }()
	defer executorServer.Stop()
	defer func() { _ = executorListener.Close() }()

	controller := core.NewExecutorController("internal-token", insecure.NewCredentials())
	defer controller.Close()
	service := core.NewServiceWithExecutorController(fixture.store, fixture.store, controller)
	job := createPolicyJob(t, fixture, "cancel-executor", "serial")
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "cancel-executor", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cancel-core", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim run: count=%d err=%v", len(claims), err)
	}
	tokenHash := sha256.Sum256([]byte("callback-token"))
	if err = fixture.store.PrepareClaimedExecutorDispatch(t.Context(), claims[0].Run, "executor-1", executorListener.Addr().String(), tokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	cancelled, err := service.CancelRun(t.Context(), &schedulerv1.CancelRunRequest{TenantId: fixture.tenantID, RunId: run.ID, Reason: "operator requested"})
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.GetStatus() != "cancelled" {
		t.Fatalf("cancelled status = %q", cancelled.GetStatus())
	}
	select {
	case request := <-recorder.requests:
		if request.GetRunId() != run.ID || request.GetReason() != "operator requested" {
			t.Fatalf("cancel request = %+v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("assigned executor did not receive cancellation")
	}
}

func TestExecutorCancellationRecoversAfterExecutorReconnects(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()

	reservation, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	executorAddress := reservation.Addr().String()
	if err = reservation.Close(); err != nil {
		t.Fatal(err)
	}

	job := createPolicyJob(t, fixture, "cancel-reconnect", "serial")
	cluster, err := fixture.store.CreateKubernetesCluster(t.Context(), store.KubernetesCluster{TenantID: fixture.tenantID, Name: "cancel-reconnect", AuthMode: "service_account", APIServer: "https://k8s.example", Namespace: "work", Credentials: store.KubernetesCredentials{Token: "reconnect-token", CAData: "reconnect-ca"}})
	if err != nil {
		t.Fatal(err)
	}
	group, err := fixture.store.CreateExecutorGroup(t.Context(), store.ExecutorGroup{TenantID: fixture.tenantID, Name: "cancel-reconnect", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	if _, err = connection.Exec(t.Context(), `UPDATE jobs SET script_language='kubernetes',script_source='{"image":"alpine:3.22"}',executor_group_id=$2,executor_handler='__kubernetes__',kubernetes_cluster_id=$3 WHERE id=$1`, job.ID, group.ID, cluster.ID); err != nil {
		t.Fatal(err)
	}
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "cancel-reconnect", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cancel-reconnect-core", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim run: count=%d err=%v", len(claims), err)
	}
	tokenHash := sha256.Sum256([]byte("reconnect-callback-token"))
	if err = fixture.store.PrepareClaimedExecutorDispatch(t.Context(), claims[0].Run, "executor-reconnect", executorAddress, tokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.CancelRun(t.Context(), fixture.tenantID, run.ID, "executor reconnect test"); err != nil {
		t.Fatal(err)
	}

	controller := core.NewExecutorController("internal-token", insecure.NewCredentials())
	engine := core.NewEngine(fixture.store, "cancel-command-core", 20*time.Millisecond, 1, "http://scheduler.invalid", 24*time.Hour, nil, core.WithExecutorController(controller))
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine.Run(engineCtx)
	defer func() {
		cancelEngine()
		engine.Wait()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var attempts int
		var lastError string
		err = connection.QueryRow(t.Context(), `SELECT attempts,COALESCE(last_error,'') FROM executor_commands WHERE run_id=$1`, run.ID).Scan(&attempts, &lastError)
		if err == nil && attempts >= 1 && lastError != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executor command was not retried while disconnected: attempts=%d error=%q query_error=%v", attempts, lastError, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	executorListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", executorAddress)
	if err != nil {
		t.Fatal(err)
	}
	executorServer := grpc.NewServer()
	recorder := &cancellationRecordingExecutor{requests: make(chan *executorv1.CancelRequest, 1)}
	executorv1.RegisterExecutorServiceServer(executorServer, recorder)
	go func() { _ = executorServer.Serve(executorListener) }()
	defer executorServer.Stop()
	defer func() { _ = executorListener.Close() }()

	select {
	case request := <-recorder.requests:
		kubernetes := request.GetKubernetesCluster()
		if request.GetRunId() != run.ID || request.GetReason() != "executor reconnect test" || request.GetExternalExecutionId() != run.ExternalExecutionID || request.GetJobId() != job.ID || request.GetScriptLanguage() != "kubernetes" || kubernetes.GetApiServer() != "https://k8s.example" || kubernetes.GetNamespace() != "work" || kubernetes.GetToken() != "reconnect-token" || kubernetes.GetCaData() != "reconnect-ca" {
			t.Fatalf("recovered cancel request = %+v", request)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not receive persisted cancellation after reconnect")
	}
	deadline = time.Now().Add(time.Second)
	for {
		var commandStatus string
		if err = connection.QueryRow(t.Context(), `SELECT status FROM executor_commands WHERE run_id=$1`, run.ID).Scan(&commandStatus); err == nil && commandStatus == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executor command status was not delivered: status=%q error=%v", commandStatus, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExecutorCompletionRecoversFromDurableOutboxAfterRestart(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "completion-outbox-restart", "serial")
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "completion-outbox-restart", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "completion-outbox-core", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim run: count=%d err=%v", len(claims), err)
	}
	const callbackToken = "durable-completion-token"
	tokenHash := sha256.Sum256([]byte(callbackToken))
	if err = fixture.store.PrepareClaimedExecutorDispatch(t.Context(), claims[0].Run, "executor-restarted", "127.0.0.1:19999", tokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	oldStore, err := executorsdk.NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	record := executorsdk.CompletionRecord{RunID: run.ID, Token: callbackToken, Succeeded: true, CreatedAt: time.Now().UTC()}
	if err = oldStore.Save(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := executorsdk.NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	executorServer, err := executorsdk.NewServer(executorsdk.Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	reporter, err := executorsdk.NewGRPCReporter(fixture.client)
	if err != nil {
		t.Fatal(err)
	}
	service, err := executorsdk.NewGRPCServer(executorServer, reporter, executorsdk.GRPCServerOptions{MaxConcurrentExecutions: 1, CompletionStore: restartedStore})
	if err != nil {
		t.Fatal(err)
	}
	deliveryCtx, cancelDelivery := context.WithCancel(t.Context())
	service.RunCompletionDelivery(deliveryCtx)
	defer func() {
		cancelDelivery()
		service.WaitCompletionDelivery()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		completed, loadErr := fixture.store.GetRun(t.Context(), fixture.tenantID, run.ID)
		if loadErr == nil && completed.Status == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("persisted executor completion was not applied: run=%+v error=%v", completed, loadErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(time.Second)
	for {
		records, listErr := restartedStore.List(t.Context())
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(records) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered completion outbox = %+v", records)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestExecutorRestartReportsInterruptedProcessLocalExecution(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "execution-inbox-restart", "serial")
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "execution-inbox-restart", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "execution-inbox-core", 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim run: count=%d err=%v", len(claims), err)
	}
	const callbackToken = "interrupted-execution-token"
	tokenHash := sha256.Sum256([]byte(callbackToken))
	if err = fixture.store.PrepareClaimedExecutorDispatch(t.Context(), claims[0].Run, "executor-restarted", "127.0.0.1:19999", tokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	oldStore, err := executorsdk.NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	request := &executorv1.DispatchRequest{RunId: run.ID, JobId: job.ID, Handler: "__script__", CallbackToken: callbackToken, TimeoutSeconds: 60, ScriptLanguage: "shell", ScriptSource: "echo must-not-rerun"}
	if err = oldStore.SaveExecution(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := executorsdk.NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	executorServer, err := executorsdk.NewServer(executorsdk.Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = executorServer.Handle("__script__", func(context.Context, executorsdk.Task) error {
		t.Fatal("process-local execution was rerun after restart")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reporter, err := executorsdk.NewGRPCReporter(fixture.client)
	if err != nil {
		t.Fatal(err)
	}
	service, err := executorsdk.NewGRPCServer(executorServer, reporter, executorsdk.GRPCServerOptions{MaxConcurrentExecutions: 1, CompletionStore: restartedStore})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.RecoverExecutions(t.Context()); err != nil {
		t.Fatal(err)
	}
	deliveryCtx, cancelDelivery := context.WithCancel(t.Context())
	service.RunCompletionDelivery(deliveryCtx)
	defer func() {
		cancelDelivery()
		service.WaitCompletionDelivery()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		failed, loadErr := fixture.store.GetRun(t.Context(), fixture.tenantID, run.ID)
		if loadErr == nil && failed.Status == "failed" && strings.Contains(failed.ErrorMessage, "executor restarted") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("interrupted execution was not failed: run=%+v error=%v", failed, loadErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCrossModuleRetryLineageThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "cross-retry", "serial")
	run, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: job.ID, IdempotencyKey: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cross-retry-core", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim = %+v, %v", claims, err)
	}
	delay := time.Duration(0)
	retry, err := fixture.store.FailRun(t.Context(), claims[0].Run, "failed", http.StatusInternalServerError, "retry me", &delay)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fixture.client.GetRun(t.Context(), &schedulerv1.GetRunRequest{TenantId: fixture.tenantID, RunId: retry.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got.TriggerType != "retry" || got.Attempt != 2 || got.RetryOfRunId != run.Id {
		t.Fatalf("retry = %+v", got)
	}
}

func TestCrossModuleExecutorRegistrationThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var group *schedulerv1.ExecutorGroup
	for _, strategy := range []string{"first", "last", "round", "random", "hash", "lfu", "lru", "failover", "busyover"} {
		created, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "cross-workers-" + strategy, RouteStrategy: strategy}})
		if err != nil {
			t.Fatalf("create %s group: %v", strategy, err)
		}
		if strategy == "round" {
			group = created
		}
	}
	groups, err := fixture.client.ListExecutorGroups(t.Context(), &schedulerv1.ListExecutorGroupsRequest{TenantId: fixture.tenantID})
	if err != nil || len(groups.Groups) != 9 {
		t.Fatalf("groups = %+v, %v", groups, err)
	}
	node, err := fixture.client.RegisterExecutorNode(t.Context(), &schedulerv1.RegisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: "worker-1", Address: "http://worker:9999", TtlSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	if !node.Online || node.NodeId != "worker-1" {
		t.Fatalf("registered node = %+v", node)
	}
	nodes, err := fixture.client.ListExecutorNodes(t.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: fixture.tenantID, GroupId: group.Id, LiveOnly: true})
	if err != nil || len(nodes.Nodes) != 1 || nodes.Nodes[0].Address != "http://worker:9999" {
		t.Fatalf("nodes = %+v, %v", nodes, err)
	}
	if _, err = fixture.client.UnregisterExecutorNode(t.Context(), &schedulerv1.UnregisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: "worker-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.client.UnregisterExecutorNode(t.Context(), &schedulerv1.UnregisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: "worker-1"}); err != nil {
		t.Fatalf("repeated unregister: %v", err)
	}
	nodes, err = fixture.client.ListExecutorNodes(t.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: fixture.tenantID, GroupId: group.Id, LiveOnly: false})
	if err != nil || len(nodes.Nodes) != 0 {
		t.Fatalf("nodes after unregister = %+v, %v", nodes, err)
	}
}

func TestCrossModuleManualExecutorGroupThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "manual-cross", RouteStrategy: "first", RegistrationMode: "manual", ManualAddresses: []string{"http://worker-b:9999/", "http://worker-a:9999"}}})
	if err != nil {
		t.Fatal(err)
	}
	if group.RegistrationMode != "manual" || strings.Join(group.ManualAddresses, ",") != "http://worker-a:9999,http://worker-b:9999" {
		t.Fatalf("manual group = %+v", group)
	}
	nodes, err := fixture.client.ListExecutorNodes(t.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: fixture.tenantID, GroupId: group.Id, LiveOnly: true})
	if err != nil || len(nodes.Nodes) != 2 || !nodes.Nodes[0].Static || !nodes.Nodes[0].Online || nodes.Nodes[0].Address != "http://worker-a:9999" {
		t.Fatalf("manual nodes = %+v, %v", nodes, err)
	}
	_, err = fixture.client.RegisterExecutorNode(t.Context(), &schedulerv1.RegisterExecutorNodeRequest{TenantId: fixture.tenantID, GroupId: group.Id, NodeId: "dynamic", Address: "http://dynamic:9999", TtlSeconds: 30})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("manual heartbeat code = %s, %v", status.Code(err), err)
	}
	job, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "manual-cross-job", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "handler"}})
	if err != nil {
		t.Fatal(err)
	}
	strategy, candidates, err := fixture.store.ExecutorRouteCandidates(t.Context(), fixture.tenantID, group.Id, job.Id)
	if err != nil || strategy != "first" || len(candidates) != 2 || candidates[0].Address != "http://worker-a:9999" || !candidates[0].Static {
		t.Fatalf("route candidates strategy=%q nodes=%+v err=%v", strategy, candidates, err)
	}
	group.Name = "manual-cross-updated"
	group.RouteStrategy = "last"
	group.ManualAddresses = []string{"http://worker-c:9999"}
	updated, err := fixture.client.UpdateExecutorGroup(t.Context(), &schedulerv1.UpdateExecutorGroupRequest{Group: group})
	if err != nil || updated.Version != group.Version+1 || len(updated.ManualAddresses) != 1 {
		t.Fatalf("updated group = %+v, %v", updated, err)
	}
	_, err = fixture.client.UpdateExecutorGroup(t.Context(), &schedulerv1.UpdateExecutorGroupRequest{Group: group})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale update code = %s, %v", status.Code(err), err)
	}
	deletable, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "manual-cross-delete", RouteStrategy: "first", RegistrationMode: "manual", ManualAddresses: []string{"http://delete:9999"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.client.DeleteExecutorGroup(t.Context(), &schedulerv1.DeleteExecutorGroupRequest{TenantId: fixture.tenantID, Id: deletable.Id, Version: deletable.Version}); err != nil {
		t.Fatal(err)
	}
}

func TestCrossModuleExecutorSDKHeartbeatThroughAPI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "sdk-heartbeat", RouteStrategy: "round"}})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "sdk-heartbeat", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	registrar, err := executorsdk.NewRegistrar(executorsdk.RegistrarOptions{APIURL: httpServer.URL, Token: token, GroupID: group.Id, NodeID: "sdk-node", Address: "http://sdk-executor:9999", TTL: 6 * time.Second, HTTPClient: httpServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- registrar.Run(runCtx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, listErr := fixture.client.ListExecutorNodes(t.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: fixture.tenantID, GroupId: group.Id, LiveOnly: true})
		if listErr == nil && len(response.Nodes) == 1 && response.Nodes[0].NodeId == "sdk-node" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("SDK node not visible through gRPC: %+v %v", response, listErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	nodes, err := fixture.client.ListExecutorNodes(t.Context(), &schedulerv1.ListExecutorNodesRequest{TenantId: fixture.tenantID, GroupId: group.Id, LiveOnly: false})
	if err != nil || len(nodes.Nodes) != 0 {
		t.Fatalf("SDK node remained after graceful shutdown: %+v %v", nodes, err)
	}
}

func TestCrossModuleScriptJobThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "script-grpc", RouteStrategy: "round"}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "script-grpc", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "__script__", ScriptLanguage: "python", ScriptSource: `print("hello")`, RequiredExecutorLabels: []string{"linux", "gpu"}, ExcludedExecutorLabels: []string{"spot"}}})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := fixture.client.GetJob(t.Context(), &schedulerv1.GetJobRequest{TenantId: fixture.tenantID, Id: created.Id})
	if err != nil || loaded.ScriptLanguage != "python" || loaded.ScriptSource != `print("hello")` || strings.Join(loaded.RequiredExecutorLabels, ",") != "gpu,linux" || strings.Join(loaded.ExcludedExecutorLabels, ",") != "spot" {
		t.Fatalf("script job=%+v err=%v", loaded, err)
	}
	for _, language := range []string{"nodejs", "php", "powershell"} {
		languageJob, createErr := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: language + "-grpc", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "__script__", ScriptLanguage: language, ScriptSource: "source"}})
		if createErr != nil || languageJob.ScriptLanguage != language {
			t.Fatalf("create %s over gRPC = %+v, %v", language, languageJob, createErr)
		}
	}
}

func TestCrossModuleScriptVersionRollbackThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.client.CreateExecutorGroup(t.Context(), &schedulerv1.CreateExecutorGroupRequest{Group: &schedulerv1.ExecutorGroup{TenantId: fixture.tenantID, Name: "script-version-grpc", RouteStrategy: "round"}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := fixture.client.CreateJob(t.Context(), &schedulerv1.CreateJobRequest{Job: &schedulerv1.Job{TenantId: fixture.tenantID, Name: "script-version-grpc", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HttpMethod: "POST", TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 30, MaxQueueSize: 10, ExecutorGroupId: group.Id, ExecutorHandler: "__script__", ScriptLanguage: "shell", ScriptSource: "printf v1"}})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := fixture.client.ListJobScriptVersions(t.Context(), &schedulerv1.ListJobScriptVersionsRequest{TenantId: fixture.tenantID, JobId: job.Id})
	if err != nil || len(initial.Versions) != 1 || initial.Versions[0].Revision != 1 {
		t.Fatalf("initial versions = %+v, %v", initial, err)
	}
	job.ScriptSource = "printf v2"
	job, err = fixture.client.UpdateJob(t.Context(), &schedulerv1.UpdateJobRequest{Job: job})
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := fixture.client.RollbackJobScriptVersion(t.Context(), &schedulerv1.RollbackJobScriptVersionRequest{TenantId: fixture.tenantID, JobId: job.Id, VersionId: initial.Versions[0].Id, JobVersion: job.Version, Remark: "grpc rollback"})
	if err != nil || rolledBack.ScriptSource != "printf v1" || rolledBack.Version != job.Version+1 {
		t.Fatalf("rollback = %+v, %v", rolledBack, err)
	}
	if _, err = fixture.client.RollbackJobScriptVersion(t.Context(), &schedulerv1.RollbackJobScriptVersionRequest{TenantId: fixture.tenantID, JobId: job.Id, VersionId: initial.Versions[0].Id, JobVersion: job.Version}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale rollback code = %v, want Aborted: %v", status.Code(err), err)
	}
	versions, err := fixture.client.ListJobScriptVersions(t.Context(), &schedulerv1.ListJobScriptVersionsRequest{TenantId: fixture.tenantID, JobId: job.Id})
	if err != nil || len(versions.Versions) != 3 || versions.Versions[0].Remark != "grpc rollback" {
		t.Fatalf("versions after rollback = %+v, %v", versions, err)
	}
}

func TestCrossModuleJobDependenciesThroughGRPC(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	parent := createPolicyJob(t, fixture, "cross-parent", "serial")
	child := createPolicyJob(t, fixture, "cross-child", "serial")
	dependencies, err := fixture.client.SetJobDependencies(t.Context(), &schedulerv1.SetJobDependenciesRequest{TenantId: fixture.tenantID, ParentJobId: parent.ID, ChildJobIds: []string{child.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies.ChildJobIds) != 1 || dependencies.ChildJobIds[0] != child.ID {
		t.Fatalf("dependencies = %+v", dependencies)
	}
	parentRun, err := fixture.client.TriggerJob(t.Context(), &schedulerv1.TriggerJobRequest{TenantId: fixture.tenantID, JobId: parent.ID, IdempotencyKey: "parent"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "dependency-cross-core", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim parent: %+v %v", claims, err)
	}
	if err = fixture.store.CompleteRun(t.Context(), claims[0].Run, true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	childRuns, err := fixture.client.ListRuns(t.Context(), &schedulerv1.ListRunsRequest{TenantId: fixture.tenantID, JobId: child.ID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(childRuns.Runs) != 1 || childRuns.Runs[0].ParentRunId != parentRun.Id {
		t.Fatalf("child runs = %+v", childRuns.Runs)
	}
}

func TestJobLifecycleUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createDisabledJob(t, fixture)
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	command := exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "jobs", "start", job.ID, "--version", fmt.Sprint(job.Version))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("schedulerctl start: %v\n%s", err, output)
	}
	var response struct {
		Enabled bool   `json:"enabled"`
		Version string `json:"version"`
	}
	if err = json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode CLI output: %v\n%s", err, output)
	}
	if !response.Enabled || response.Version != fmt.Sprint(job.Version+1) {
		t.Fatalf("unexpected CLI response: %+v", response)
	}
	stored, err := fixture.store.GetJob(t.Context(), fixture.tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Enabled || stored.NextRunAt == nil {
		t.Fatalf("CLI did not make job schedulable: %+v", stored)
	}
	command = exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "jobs", "stop", job.ID, "--version", fmt.Sprint(stored.Version))
	output, err = command.CombinedOutput()
	if err != nil {
		t.Fatalf("schedulerctl stop: %v\n%s", err, output)
	}
	stored, err = fixture.store.GetJob(t.Context(), fixture.tenantID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Enabled || stored.NextRunAt != nil {
		t.Fatalf("CLI did not stop future scheduling: %+v", stored)
	}
}

func TestSchedulePreviewUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "preview-e2e", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	command := exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "jobs", "preview", "--type", "cron", "--expression", "0 0 9 L * ?", "--timezone", "Asia/Shanghai", "--after", "2026-08-13T00:00:00Z", "--count", "5")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("schedulerctl preview: %v\n%s", err, output)
	}
	var response struct {
		TriggerTimes []string `json:"trigger_times"`
	}
	if err = json.Unmarshal(output, &response); err != nil || len(response.TriggerTimes) != 5 || response.TriggerTimes[0] != "2026-08-31T01:00:00Z" || response.TriggerTimes[4] != "2026-12-31T01:00:00Z" {
		t.Fatalf("preview output=%s err=%v", output, err)
	}
	var jobCount, runCount int
	conn, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	if err = conn.QueryRow(t.Context(), `SELECT count(*) FROM jobs WHERE tenant_id=$1`, fixture.tenantID).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if err = conn.QueryRow(t.Context(), `SELECT count(*) FROM job_runs WHERE tenant_id=$1`, fixture.tenantID).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 0 || runCount != 0 {
		t.Fatalf("preview mutated state jobs=%d runs=%d", jobCount, runCount)
	}
}

func TestMultiCoreFailoverUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	etcd := newEtcdFixture(t)
	prefix := "/ha-usecase/services"
	first := startHACore(t, etcd, fixture.dsn, prefix, "core-1", fixture.cipher)
	second := startHACore(t, etcd, fixture.dsn, prefix, "core-2", fixture.cipher)
	client, conn := newDiscoveredCoreClient(t, etcd, prefix)
	defer conn.Close()
	var executions atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { executions.Add(1); w.WriteHeader(http.StatusNoContent) }))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "ha-e2e", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "ha-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) ([]byte, error) {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		return command.CombinedOutput()
	}
	engineCtx1, cancelEngine1 := context.WithCancel(t.Context())
	engineCtx2, cancelEngine2 := context.WithCancel(t.Context())
	engine1 := core.NewEngine(first.store, "ha-core-1", 10*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine2 := core.NewEngine(second.store, "ha-core-2", 10*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine1.Run(engineCtx1)
	engine2.Run(engineCtx2)
	defer func() { cancelEngine1(); cancelEngine2(); engine1.Wait(); engine2.Wait() }()
	triggerAndWait := func(key string, want int32) {
		t.Helper()
		output, triggerErr := runCLI("jobs", "trigger", job.ID, "--idempotency-key", key)
		if triggerErr != nil {
			t.Fatalf("trigger %s: %v\n%s", key, triggerErr, output)
		}
		deadline := time.Now().Add(5 * time.Second)
		for executions.Load() < want && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if executions.Load() != want {
			t.Fatalf("executions after %s = %d, want %d", key, executions.Load(), want)
		}
	}
	triggerAndWait("before-failover", 1)
	time.Sleep(200 * time.Millisecond)
	if executions.Load() != 1 {
		t.Fatalf("duplicate execution before failover: %d", executions.Load())
	}
	cancelEngine1()
	engine1.Wait()
	first.stop()
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, getErr := etcd.Get(t.Context(), prefix+"/scheduler-core/", clientv3.WithPrefix())
		if getErr == nil && len(response.Kvs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed core remained in discovery")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		_, listErr := client.ListJobs(ctx, &schedulerv1.ListJobsRequest{TenantId: fixture.tenantID, Limit: 10})
		cancel()
		if listErr != nil {
			t.Fatalf("resolver did not fail over: %v", listErr)
		}
	}
	triggerAndWait("after-failover", 2)
	time.Sleep(200 * time.Millisecond)
	if executions.Load() != 2 {
		t.Fatalf("duplicate execution after failover: %d", executions.Load())
	}
	runsOutput, err := runCLI("runs", "--job", job.ID)
	if err != nil {
		t.Fatalf("list runs after failover: %v\n%s", err, runsOutput)
	}
	var listed struct {
		Runs []struct {
			Status string `json:"status"`
		} `json:"runs"`
	}
	if err = json.Unmarshal(runsOutput, &listed); err != nil || len(listed.Runs) != 2 || listed.Runs[0].Status != "succeeded" || listed.Runs[1].Status != "succeeded" {
		t.Fatalf("runs after failover = %s, %v", runsOutput, err)
	}
}

func TestExecutorSDKUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "executor-sdk-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	sdk, err := executorsdk.NewServer(executorsdk.Options{SchedulerURL: httpServer.URL, HTTPClient: httpServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan executorsdk.Task, 1)
	if err = sdk.Handle("invoiceHandler", func(_ context.Context, task executorsdk.Task) error {
		if logErr := task.Logger.Info("invoice " + task.Input); logErr != nil {
			return logErr
		}
		handled <- task
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = sdk.Handle("__script__", executorsdk.ScriptHandler(executorsdk.ScriptOptions{Languages: []string{"shell", "python", "nodejs", "php", "powershell"}})); err != nil {
		t.Fatal(err)
	}
	reporter, err := executorsdk.NewGRPCReporter(fixture.client)
	if err != nil {
		t.Fatal(err)
	}
	grpcService, err := executorsdk.NewGRPCServer(sdk, reporter)
	if err != nil {
		t.Fatal(err)
	}
	executorListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	executorGRPC := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(executorGRPC, grpcService)
	go func() { _ = executorGRPC.Serve(executorListener) }()
	defer executorGRPC.Stop()
	defer func() { _ = executorListener.Close() }()
	executorAddress := "grpc://" + executorListener.Addr().String()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupOutput := runCLI(nil, "executors", "groups", "create", "--name", "sdk-e2e", "--strategy", "round")
	var group struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(groupOutput, &group); err != nil || group.ID == "" {
		t.Fatalf("group=%s %v", groupOutput, err)
	}
	registrar, err := executorsdk.NewRegistrar(executorsdk.RegistrarOptions{APIURL: httpServer.URL, Token: token, GroupID: group.ID, NodeID: "sdk-e2e-node", Address: executorAddress, TTL: 6 * time.Second, HTTPClient: httpServer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	registrarCtx, cancelRegistrar := context.WithCancel(t.Context())
	registrarDone := make(chan error, 1)
	go func() { registrarDone <- registrar.Run(registrarCtx) }()
	registrarStopped := false
	defer func() {
		if !registrarStopped {
			cancelRegistrar()
			<-registrarDone
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		nodes := runCLI(nil, "executors", "list", group.ID)
		if bytes.Contains(nodes, []byte("sdk-e2e-node")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executor did not register: %s", nodes)
		}
		time.Sleep(20 * time.Millisecond)
	}
	definition := fmt.Sprintf(`{"name":"sdk-handler","schedule_type":"fixed_rate","schedule_expression":"60","timezone":"UTC","http_method":"POST","timeout_seconds":5,"max_retries":0,"overlap_policy":"parallel","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":30,"max_queue_size":10,"executor_group_id":%q,"executor_handler":"invoiceHandler","enabled":false}`, group.ID)
	jobOutput := runCLI([]byte(definition), "jobs", "create", "--file", "-")
	var job struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(jobOutput, &job); err != nil || job.ID == "" {
		t.Fatalf("job=%s %v", jobOutput, err)
	}
	triggerOutput := runCLI(nil, "jobs", "trigger", job.ID, "--idempotency-key", "sdk-e2e", "--input", "INV-42")
	var run struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(triggerOutput, &run); err != nil || run.ID == "" {
		t.Fatalf("run=%s %v", triggerOutput, err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "sdk-e2e-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("sdk-e2e-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	select {
	case task := <-handled:
		if task.Input != "INV-42" || task.RunID != run.ID {
			t.Fatalf("task=%+v", task)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SDK handler was not invoked")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		output := runCLI(nil, "runs", "get", run.ID)
		if bytes.Contains(output, []byte(`"status": "succeeded"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not succeed: %s", output)
		}
		time.Sleep(20 * time.Millisecond)
	}
	logs := runCLI(nil, "runs", "logs", run.ID)
	if !bytes.Contains(logs, []byte("invoice INV-42")) || !bytes.Contains(logs, []byte(`"stream": "stdout"`)) {
		t.Fatalf("SDK logs=%s", logs)
	}
	scriptDefinition := fmt.Sprintf(`{"name":"shell-script","schedule_type":"fixed_rate","schedule_expression":"60","timezone":"UTC","http_method":"POST","timeout_seconds":5,"max_retries":0,"overlap_policy":"parallel","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":30,"max_queue_size":10,"executor_group_id":%q,"executor_handler":"__script__","script_language":"shell","script_source":"printf 'script:%s' \"$SCHEDULER_INPUT\"; printf 'warn' >&2","enabled":false}`, group.ID, "%s")
	scriptJobOutput := runCLI([]byte(scriptDefinition), "jobs", "create", "--file", "-")
	var scriptJob struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(scriptJobOutput, &scriptJob); err != nil || scriptJob.ID == "" {
		t.Fatalf("script job=%s %v", scriptJobOutput, err)
	}
	versionsOutput := runCLI(nil, "jobs", "script-versions", "list", scriptJob.ID)
	var initialVersions struct {
		Versions []struct {
			ID       string `json:"id"`
			Revision string `json:"revision"`
		} `json:"versions"`
	}
	if err = json.Unmarshal(versionsOutput, &initialVersions); err != nil || len(initialVersions.Versions) != 1 || initialVersions.Versions[0].Revision != "1" {
		t.Fatalf("initial script versions=%s %v", versionsOutput, err)
	}
	currentOutput := runCLI(nil, "jobs", "get", scriptJob.ID)
	var current map[string]any
	if err = json.Unmarshal(currentOutput, &current); err != nil {
		t.Fatal(err)
	}
	current["script_source"] = "printf 'wrong-version'"
	updateInput, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	updatedScriptOutput := runCLI(updateInput, "jobs", "update", scriptJob.ID, "--file", "-")
	var updatedScript map[string]any
	if err = json.Unmarshal(updatedScriptOutput, &updatedScript); err != nil {
		t.Fatalf("updated script=%s %v", updatedScriptOutput, err)
	}
	jobVersion, _ := updatedScript["version"].(string)
	rolledBackOutput := runCLI(nil, "jobs", "script-versions", "rollback", scriptJob.ID, initialVersions.Versions[0].ID, "--version", jobVersion, "--remark", "restore e2e source")
	if !bytes.Contains(rolledBackOutput, []byte(`"script_source": "printf 'script:%s'`)) {
		t.Fatalf("rolled back script=%s", rolledBackOutput)
	}
	versionsOutput = runCLI(nil, "jobs", "script-versions", "list", scriptJob.ID)
	if !bytes.Contains(versionsOutput, []byte(`"revision": "3"`)) || !bytes.Contains(versionsOutput, []byte(`"remark": "restore e2e source"`)) {
		t.Fatalf("versions after rollback=%s", versionsOutput)
	}
	scriptRunOutput := runCLI(nil, "jobs", "trigger", scriptJob.ID, "--idempotency-key", "script-e2e", "--input", "PAYLOAD")
	var scriptRun struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(scriptRunOutput, &scriptRun); err != nil || scriptRun.ID == "" {
		t.Fatalf("script run=%s %v", scriptRunOutput, err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		output := runCLI(nil, "runs", "get", scriptRun.ID)
		if bytes.Contains(output, []byte(`"status": "succeeded"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("script run did not succeed: %s", output)
		}
		time.Sleep(20 * time.Millisecond)
	}
	scriptLogs := runCLI(nil, "runs", "logs", scriptRun.ID)
	if !bytes.Contains(scriptLogs, []byte("script:PAYLOAD")) || !bytes.Contains(scriptLogs, []byte("warn")) || !bytes.Contains(scriptLogs, []byte(`"stream": "stderr"`)) {
		t.Fatalf("script logs=%s", scriptLogs)
	}
	additionalScripts := []struct {
		language, source, output string
	}{
		{language: "nodejs", source: `process.stdout.write("node:" + process.env.SCHEDULER_INPUT)`, output: "node:PAYLOAD"},
		{language: "php", source: `<?php fwrite(STDOUT, "php:" . getenv("SCHEDULER_INPUT"));`, output: "php:PAYLOAD"},
	}
	for _, script := range additionalScripts {
		definition := fmt.Sprintf(`{"name":%q,"schedule_type":"fixed_rate","schedule_expression":"60","timezone":"UTC","http_method":"POST","timeout_seconds":5,"max_retries":0,"overlap_policy":"parallel","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":30,"max_queue_size":10,"executor_group_id":%q,"executor_handler":"__script__","script_language":%q,"script_source":%q,"enabled":false}`, script.language+"-script", group.ID, script.language, script.source)
		jobOutput := runCLI([]byte(definition), "jobs", "create", "--file", "-")
		var languageJob struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(jobOutput, &languageJob); err != nil || languageJob.ID == "" {
			t.Fatalf("%s job=%s %v", script.language, jobOutput, err)
		}
		runOutput := runCLI(nil, "jobs", "trigger", languageJob.ID, "--idempotency-key", script.language+"-e2e", "--input", "PAYLOAD")
		var languageRun struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(runOutput, &languageRun); err != nil || languageRun.ID == "" {
			t.Fatalf("%s run=%s %v", script.language, runOutput, err)
		}
		deadline = time.Now().Add(5 * time.Second)
		for {
			output := runCLI(nil, "runs", "get", languageRun.ID)
			if bytes.Contains(output, []byte(`"status": "succeeded"`)) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s run did not succeed: %s", script.language, output)
			}
			time.Sleep(20 * time.Millisecond)
		}
		logs := runCLI(nil, "runs", "logs", languageRun.ID)
		if !bytes.Contains(logs, []byte(script.output)) {
			t.Fatalf("%s logs=%s", script.language, logs)
		}
	}
	cancelRegistrar()
	if err = <-registrarDone; err != nil {
		t.Fatal(err)
	}
	registrarStopped = true
	nodes := runCLI(nil, "executors", "list", group.ID)
	if bytes.Contains(nodes, []byte("sdk-e2e-node")) {
		t.Fatalf("executor remained visible after graceful shutdown: %s", nodes)
	}
}

func TestPowerShellScriptExecutorImageUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "powershell-image-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	apiServer := &http.Server{Handler: apihttp.NewServer(fixture.client, manager, false).Routes(), ReadHeaderTimeout: time.Second}
	go func() { _ = apiServer.Serve(listener) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = apiServer.Shutdown(shutdownCtx)
	})
	apiPort := listener.Addr().(*net.TCPAddr).Port
	hostAPIURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	containerAPIURL := fmt.Sprintf("http://%s:%d", testcontainers.HostInternal, apiPort)
	grpcListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(rpc.UnaryServerAuth(token, "")))
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, core.NewService(fixture.store))
	go func() { _ = grpcServer.Serve(grpcListener) }()
	t.Cleanup(func() { grpcServer.Stop() })
	containerGRPCAddress := net.JoinHostPort(testcontainers.HostInternal, fmt.Sprint(grpcListener.Addr().(*net.TCPAddr).Port))

	group, err := fixture.store.CreateExecutorGroup(t.Context(), store.ExecutorGroup{TenantID: fixture.tenantID, Name: "powershell-image-e2e", RouteStrategy: "first"})
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	executorContainer, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: root, Dockerfile: "deploy/script-executor/Dockerfile", Repo: "go-scheduler-script-executor-test", Tag: "powershell-usecase", KeepImage: true},
		Env:            map[string]string{"SCHEDULER_GRPC_ADDRESS": containerGRPCAddress, "SCHEDULER_TOKEN": token, "EXECUTOR_TENANT_ID": fixture.tenantID, "EXECUTOR_GROUP_ID": group.ID, "EXECUTOR_NODE_ID": "powershell-image", "EXECUTOR_ADVERTISE_ADDRESS": "grpc://executor:9999", "EXECUTOR_LISTEN": ":9999", "EXECUTOR_TTL": "6s", "SCRIPT_LANGUAGES": "powershell"},
		ExposedPorts:   []string{"9999/tcp"},
		ExtraHosts:     []string{testcontainers.HostInternal + ":host-gateway"},
		WaitingFor:     waitpkg.ForListeningPort("9999/tcp"),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = executorContainer.Terminate(context.Background()) })
	host, err := executorContainer.Host(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	mappedPort, err := executorContainer.MappedPort(t.Context(), "9999/tcp")
	if err != nil {
		t.Fatal(err)
	}
	executorURL := "grpc://" + net.JoinHostPort(host, mappedPort.Port())
	// The container advertises its compose-style address for real deployments. Add
	// the host-mapped address under a lexically earlier node ID so this host-side
	// integration test deterministically selects the reachable endpoint.
	if _, err = fixture.store.RegisterExecutorNode(t.Context(), fixture.tenantID, group.ID, "000-powershell-image-host", executorURL, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", hostAPIURL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	definition := fmt.Sprintf(`{"name":"powershell-image","schedule_type":"fixed_rate","schedule_expression":"60","timezone":"UTC","http_method":"POST","timeout_seconds":10,"max_retries":0,"overlap_policy":"parallel","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":30,"max_queue_size":10,"executor_group_id":%q,"executor_handler":"__script__","script_language":"powershell","script_source":"[Console]::Out.Write('pwsh:' + $env:SCHEDULER_INPUT); [Console]::Error.Write('pwsh-err')","enabled":false}`, group.ID)
	jobOutput := runCLI([]byte(definition), "jobs", "create", "--file", "-")
	var job struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(jobOutput, &job); err != nil || job.ID == "" {
		t.Fatalf("job=%s %v", jobOutput, err)
	}
	runOutput := runCLI(nil, "jobs", "trigger", job.ID, "--idempotency-key", "powershell-image", "--input", "PAYLOAD")
	var run struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(runOutput, &run); err != nil || run.ID == "" {
		t.Fatalf("run=%s %v", runOutput, err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "powershell-image-core", 20*time.Millisecond, 1, containerAPIURL, 90*24*time.Hour, nil, core.WithExecutorGRPC(token))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		output := runCLI(nil, "runs", "get", run.ID)
		if bytes.Contains(output, []byte(`"status": "succeeded"`)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PowerShell run did not succeed: %s", output)
		}
		time.Sleep(50 * time.Millisecond)
	}
	logs := runCLI(nil, "runs", "logs", run.ID)
	if !bytes.Contains(logs, []byte("pwsh:PAYLOAD")) || !bytes.Contains(logs, []byte("pwsh-err")) {
		t.Fatalf("PowerShell logs=%s", logs)
	}
}

func TestManualExecutorGroupUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var firstCalls, secondCalls atomic.Int32
	firstExecutor := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"manual.handler": countingHandler(&firstCalls)})
	secondExecutor := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"manual.handler": countingHandler(&secondCalls)})
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "manual-executor-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input string, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		if input != "" {
			command.Stdin = strings.NewReader(input)
		}
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupOutput := runCLI("", "executors", "groups", "create", "--name", "manual-e2e", "--strategy", "first", "--mode", "manual", "--address", firstExecutor)
	var group struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	}
	if err = json.Unmarshal(groupOutput, &group); err != nil || group.ID == "" || group.Version != "1" {
		t.Fatalf("group output=%s err=%v", groupOutput, err)
	}
	definition := fmt.Sprintf(`{"name":"manual-e2e-job","schedule_type":"fixed_rate","schedule_expression":"60","timezone":"UTC","http_method":"POST","timeout_seconds":5,"max_retries":0,"overlap_policy":"parallel","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":30,"max_queue_size":10,"executor_group_id":%q,"executor_handler":"manual.handler","enabled":false}`, group.ID)
	jobOutput := runCLI(definition, "jobs", "create", "--file", "-")
	var job struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	}
	if err = json.Unmarshal(jobOutput, &job); err != nil || job.ID == "" {
		t.Fatalf("job output=%s err=%v", jobOutput, err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "manual-e2e-core", 10*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	runCLI("", "jobs", "trigger", job.ID, "--idempotency-key", "manual-first")
	waitFor := func(calls *atomic.Int32, want int32) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for calls.Load() < want && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if calls.Load() != want {
			t.Fatalf("executor calls=%d want=%d", calls.Load(), want)
		}
	}
	waitFor(&firstCalls, 1)
	updatedOutput := runCLI("", "executors", "groups", "update", group.ID, "--name", "manual-e2e", "--strategy", "first", "--mode", "manual", "--address", secondExecutor, "--version", group.Version)
	var updatedGroup struct {
		Version string `json:"version"`
	}
	if err = json.Unmarshal(updatedOutput, &updatedGroup); err != nil || updatedGroup.Version != "2" {
		t.Fatalf("updated group=%s err=%v", updatedOutput, err)
	}
	runCLI("", "jobs", "trigger", job.ID, "--idempotency-key", "manual-second")
	waitFor(&secondCalls, 1)
	if firstCalls.Load() != 1 {
		t.Fatalf("old manual address reused: first calls=%d", firstCalls.Load())
	}
	runCLI("", "jobs", "delete", job.ID, "--version", job.Version)
	runCLI("", "executors", "groups", "delete", group.ID, "--version", updatedGroup.Version)
	groupsOutput := runCLI("", "executors", "groups", "list")
	if bytes.Contains(groupsOutput, []byte(group.ID)) {
		t.Fatalf("deleted group remains: %s", groupsOutput)
	}
}

func TestRunReportUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "report-e2e", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, MaxRetries: 0, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "report-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	runCLI("jobs", "trigger", job.ID, "--idempotency-key", "report-success")
	runCLI("jobs", "trigger", job.ID, "--idempotency-key", "report-failure")
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "report-core", 20*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	fixture.useEngine(engine)
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		var succeeded, failed int
		for _, run := range runs {
			if run.Status == "succeeded" {
				succeeded++
			}
			if run.Status == "failed" || run.Status == "timed_out" {
				failed++
			}
		}
		if succeeded == 1 && failed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("executor calls = %d runs=%+v", calls.Load(), runs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	today := time.Now().UTC().Format(time.DateOnly)
	output := runCLI("reports", "runs", "--from", today, "--to", today, "--timezone", "UTC")
	var report struct {
		Points []struct {
			Total     string `json:"total"`
			Succeeded string `json:"succeeded"`
			Failed    string `json:"failed"`
		} `json:"points"`
	}
	if err = json.Unmarshal(output, &report); err != nil {
		t.Fatalf("decode report: %v\n%s", err, output)
	}
	if len(report.Points) != 1 || report.Points[0].Total != "2" || report.Points[0].Succeeded != "1" || report.Points[0].Failed != "1" {
		t.Fatalf("report = %s", output)
	}
	before := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	purgeOutput := runCLI("runs", "purge", "--before", before, "--job", job.ID, "--limit", "1")
	var purged struct {
		Deleted string `json:"deleted"`
	}
	if err = json.Unmarshal(purgeOutput, &purged); err != nil || purged.Deleted != "1" {
		t.Fatalf("first purge = %s, %v", purgeOutput, err)
	}
	remaining := runCLI("runs", "--job", job.ID)
	var runs struct {
		Runs []json.RawMessage `json:"runs"`
	}
	if err = json.Unmarshal(remaining, &runs); err != nil || len(runs.Runs) != 1 {
		t.Fatalf("remaining runs = %s, %v", remaining, err)
	}
	purgeOutput = runCLI("runs", "purge", "--before", before, "--job", job.ID, "--limit", "10")
	if err = json.Unmarshal(purgeOutput, &purged); err != nil || purged.Deleted != "1" {
		t.Fatalf("second purge = %s, %v", purgeOutput, err)
	}
}

func TestJobCRUDUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "crud-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) ([]byte, error) {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		return command.CombinedOutput()
	}
	groupID := attachHTTPExecutor(t, fixture)
	definition := []byte(fmt.Sprintf(`{"name":"crud-e2e","description":"created","schedule_type":"fixed_interval","schedule_expression":"60","timezone":"UTC","target_url":"https://example.com/jobs","http_method":"POST","headers":{"X-Source":"cli"},"timeout_seconds":30,"max_retries":1,"overlap_policy":"serial","misfire_policy":"fire_once","max_concurrent_runs":1,"max_catch_up":10,"callback_timeout_seconds":3600,"max_queue_size":1000,"executor_group_id":%q,"enabled":false}`, groupID))
	createdOutput, err := runCLI(definition, "jobs", "create", "--file", "-")
	if err != nil {
		t.Fatalf("create job: %v\n%s", err, createdOutput)
	}
	var created map[string]any
	if err = json.Unmarshal(createdOutput, &created); err != nil {
		t.Fatalf("decode created job: %v\n%s", err, createdOutput)
	}
	jobID, _ := created["id"].(string)
	staleVersion, _ := created["version"].(string)
	if jobID == "" || staleVersion == "" {
		t.Fatalf("created job lacks id/version: %s", createdOutput)
	}
	listOutput, err := runCLI(nil, "jobs", "list")
	if err != nil || !bytes.Contains(listOutput, []byte(jobID)) {
		t.Fatalf("list jobs: %v\n%s", err, listOutput)
	}
	getOutput, err := runCLI(nil, "jobs", "get", jobID)
	if err != nil || !bytes.Contains(getOutput, []byte(`"name": "crud-e2e"`)) {
		t.Fatalf("get job: %v\n%s", err, getOutput)
	}
	created["name"] = "crud-e2e-updated"
	created["description"] = "updated"
	delete(created, "headers")
	updateInput, err := json.Marshal(created)
	if err != nil {
		t.Fatal(err)
	}
	updatedOutput, err := runCLI(updateInput, "jobs", "update", jobID, "--file", "-")
	if err != nil {
		t.Fatalf("update job: %v\n%s", err, updatedOutput)
	}
	var updated map[string]any
	if err = json.Unmarshal(updatedOutput, &updated); err != nil {
		t.Fatal(err)
	}
	updatedVersion, _ := updated["version"].(string)
	if updated["name"] != "crud-e2e-updated" || updatedVersion == staleVersion {
		t.Fatalf("updated job = %s", updatedOutput)
	}
	stored, err := fixture.store.GetJob(t.Context(), fixture.tenantID, jobID)
	if err != nil || stored.Headers["X-Source"] != "cli" {
		t.Fatalf("update did not preserve hidden headers: %+v, %v", stored.Headers, err)
	}
	staleOutput, staleErr := runCLI(updateInput, "jobs", "update", jobID, "--file", "-")
	if staleErr == nil || !bytes.Contains(staleOutput, []byte("HTTP 409")) {
		t.Fatalf("stale update = %v\n%s", staleErr, staleOutput)
	}
	deleteOutput, err := runCLI(nil, "jobs", "delete", jobID, "--version", updatedVersion)
	if err != nil {
		t.Fatalf("delete job: %v\n%s", err, deleteOutput)
	}
	missingOutput, missingErr := runCLI(nil, "jobs", "get", jobID)
	if missingErr == nil || !bytes.Contains(missingOutput, []byte("HTTP 404")) {
		t.Fatalf("get deleted job = %v\n%s", missingErr, missingOutput)
	}
}

func TestManualTriggerIdempotencyUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	bodies := make(chan string, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "manual-idempotency-e2e", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, BodyTemplate: "{{input}}", TimeoutSeconds: 5, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "manual-trigger-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) ([]byte, error) {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		return command.CombinedOutput()
	}
	firstOutput, err := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "deploy-42", "--input", "first-payload")
	if err != nil {
		t.Fatalf("first trigger: %v\n%s", err, firstOutput)
	}
	secondOutput, err := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "deploy-42", "--input", "second-payload")
	if err != nil {
		t.Fatalf("second trigger: %v\n%s", err, secondOutput)
	}
	var first, second struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(firstOutput, &first); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(secondOutput, &second); err != nil || first.ID == "" || first.ID != second.ID {
		t.Fatalf("trigger outputs differ: %s\n%s", firstOutput, secondOutput)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "manual-trigger-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	select {
	case body := <-bodies:
		if body != "first-payload" {
			t.Fatalf("executed body = %q, want first-payload", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("manual run was not executed")
	}
	select {
	case duplicate := <-bodies:
		t.Fatalf("manual run executed twice, second body=%q", duplicate)
	case <-time.After(300 * time.Millisecond):
	}
	runsOutput, err := runCLI("runs", "--job", job.ID)
	if err != nil {
		t.Fatalf("list runs: %v\n%s", err, runsOutput)
	}
	var listed struct {
		Runs []json.RawMessage `json:"runs"`
	}
	if err = json.Unmarshal(runsOutput, &listed); err != nil || len(listed.Runs) != 1 {
		t.Fatalf("runs = %s, %v", runsOutput, err)
	}
	missingOutput, missingErr := runCLI("jobs", "trigger", "00000000-0000-0000-0000-000000000099", "--idempotency-key", "missing")
	if missingErr == nil || !bytes.Contains(missingOutput, []byte("HTTP 404")) {
		t.Fatalf("missing trigger = %v\n%s", missingErr, missingOutput)
	}
}

func TestAsyncCallbackRetryUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	type callbackDispatch struct {
		URL   string
		Token string
		RunID string
	}
	dispatches := make(chan callbackDispatch, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatches <- callbackDispatch{URL: r.Header.Get("X-Job-Callback-URL"), Token: r.Header.Get("X-Job-Callback-Token"), RunID: r.Header.Get("X-Job-Run-ID")}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer target.Close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "callback-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupID := attachHTTPExecutor(t, fixture)
	definition, err := json.Marshal(map[string]any{"name": "callback-retry-e2e", "schedule_type": "fixed_rate", "schedule_expression": "60", "timezone": "UTC", "target_url": target.URL, "http_method": "POST", "timeout_seconds": 5, "max_retries": 1, "overlap_policy": "parallel", "misfire_policy": "fire_once", "max_concurrent_runs": 1, "max_catch_up": 10, "callback_timeout_seconds": 10, "max_queue_size": 10, "enabled": false, "executor_group_id": groupID})
	if err != nil {
		t.Fatal(err)
	}
	createdOutput := runCLI(definition, "jobs", "create", "--file", "-")
	var created struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(createdOutput, &created); err != nil || created.ID == "" {
		t.Fatalf("created job = %s, %v", createdOutput, err)
	}
	runCLI(nil, "jobs", "trigger", created.ID, "--idempotency-key", "callback-e2e", "--input", "payload")
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "callback-e2e-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	waitDispatch := func() callbackDispatch {
		select {
		case dispatch := <-dispatches:
			if dispatch.URL == "" || dispatch.Token == "" || dispatch.RunID == "" {
				t.Fatalf("incomplete callback dispatch: %+v", dispatch)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				run, loadErr := fixture.store.GetRun(t.Context(), fixture.tenantID, dispatch.RunID)
				if loadErr == nil && run.Status == "waiting_callback" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("run did not enter waiting_callback: %+v, %v", run, loadErr)
				}
				time.Sleep(10 * time.Millisecond)
			}
			return dispatch
		case <-time.After(5 * time.Second):
			t.Fatal("executor was not dispatched")
			return callbackDispatch{}
		}
	}
	postCallback := func(dispatch callbackDispatch, succeeded bool, message string) int {
		payload, marshalErr := json.Marshal(map[string]any{"token": dispatch.Token, "succeeded": succeeded, "message": message})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		response, postErr := http.Post(dispatch.URL, "application/json", bytes.NewReader(payload))
		if postErr != nil {
			t.Fatal(postErr)
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	first := waitDispatch()
	if statusCode := postCallback(first, false, "first async failure"); statusCode != http.StatusOK {
		t.Fatalf("failed callback status = %d", statusCode)
	}
	second := waitDispatch()
	if second.RunID == first.RunID {
		t.Fatalf("retry reused run ID %s", first.RunID)
	}
	if statusCode := postCallback(second, true, "done"); statusCode != http.StatusOK {
		t.Fatalf("successful callback status = %d", statusCode)
	}
	if statusCode := postCallback(first, true, "replay"); statusCode != http.StatusNotFound {
		t.Fatalf("replayed callback status = %d, want 404", statusCode)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runsOutput := runCLI(nil, "runs", "--job", created.ID)
		var listed struct {
			Runs []struct {
				ID           string `json:"id"`
				Status       string `json:"status"`
				Attempt      int32  `json:"attempt"`
				RetryOfRunID string `json:"retry_of_run_id"`
			} `json:"runs"`
		}
		if err = json.Unmarshal(runsOutput, &listed); err != nil {
			t.Fatal(err)
		}
		if len(listed.Runs) == 2 {
			states := make(map[int32]string, 2)
			for _, run := range listed.Runs {
				states[run.Attempt] = run.Status
			}
			if states[1] == "failed" && states[2] == "succeeded" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback runs did not finish: %s", runsOutput)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestBlockPolicyUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	job := createPolicyJob(t, fixture, "cli-discard", "discard_later")
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "block-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		base := []string{"--server", httpServer.URL, "--token", token}
		command := exec.CommandContext(t.Context(), binary, append(base, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	firstOutput := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "first")
	var first struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(firstOutput, &first); err != nil {
		t.Fatal(err)
	}
	claims, err := fixture.store.ClaimRuns(t.Context(), "cli-core", 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim first run: count=%d err=%v", len(claims), err)
	}
	secondOutput := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "second")
	var second struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err = json.Unmarshal(secondOutput, &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != "skipped" {
		t.Fatalf("second trigger status = %q, want skipped", second.Status)
	}
	runsOutput := runCLI("runs", "--job", job.ID)
	if !bytes.Contains(runsOutput, []byte(first.ID)) || !bytes.Contains(runsOutput, []byte(second.ID)) {
		t.Fatalf("CLI runs omitted policy results: %s", runsOutput)
	}
}

func TestCoverEarlyExecutionUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	firstStarted := make(chan struct{})
	firstCancelled := make(chan struct{})
	var requests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-r.Context().Done()
			close(firstCancelled)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("replacement completed"))
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "cli-cover-execution", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: "cover_early", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "cover-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(key string) {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "jobs", "trigger", job.ID, "--idempotency-key", key)
		if output, commandErr := command.CombinedOutput(); commandErr != nil {
			t.Fatalf("schedulerctl trigger: %v\n%s", commandErr, output)
		}
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "cover-core", 20*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	fixture.useEngine(engine)
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	runCLI("cover-first")
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first execution did not start")
	}
	runCLI("cover-second")
	select {
	case <-firstCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("covered execution did not receive cancellation")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		var cancelled, succeeded bool
		for _, run := range runs {
			cancelled = cancelled || run.Status == "cancelled"
			succeeded = succeeded || run.Status == "succeeded"
		}
		if cancelled && succeeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement did not complete, runs=%+v", runs)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestCancelRunningUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "cli-cancel-running", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "cancel-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCommand := exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "jobs", "trigger", job.ID, "--idempotency-key", "cancel-running")
	runOutput, err := runCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("trigger run: %v\n%s", err, runOutput)
	}
	var triggered struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(runOutput, &triggered); err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "cancel-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not start")
	}
	cancelCommand := exec.CommandContext(t.Context(), binary, "--server", httpServer.URL, "--token", token, "runs", "cancel", triggered.ID, "--reason", "operator requested")
	cancelOutput, err := cancelCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("cancel run: %v\n%s", err, cancelOutput)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("running HTTP request was not cancelled")
	}
	var result struct {
		Status       string `json:"status"`
		ErrorMessage string `json:"error_message"`
	}
	if err = json.Unmarshal(cancelOutput, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "cancelled" || result.ErrorMessage != "operator requested" {
		t.Fatalf("cancel result = %+v", result)
	}
}

func TestJobDependencyUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var requests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	parent, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "cli-parent", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "cli-child", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 30, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "dependency-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		base := []string{"--server", httpServer.URL, "--token", token}
		command := exec.CommandContext(t.Context(), binary, append(base, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	runCLI("jobs", "dependencies", "set", parent.ID, "--child", child.ID)
	dependencyOutput := runCLI("jobs", "dependencies", "get", parent.ID)
	if !bytes.Contains(dependencyOutput, []byte(child.ID)) {
		t.Fatalf("dependency output = %s", dependencyOutput)
	}
	parentOutput := runCLI("jobs", "trigger", parent.ID, "--idempotency-key", "dependency-parent")
	var parentRun struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(parentOutput, &parentRun); err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "dependency-core", 20*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		childRuns, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, child.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(childRuns) == 1 && childRuns[0].Status == "succeeded" && childRuns[0].ParentRunID == parentRun.ID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child did not complete: %+v requests=%d", childRuns, requests.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	runsOutput := runCLI("runs", "--job", child.ID)
	if !bytes.Contains(runsOutput, []byte(parentRun.ID)) {
		t.Fatalf("CLI omitted parent_run_id: %s", runsOutput)
	}
}

func TestTimeoutRetryUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var requests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("retry succeeded"))
	}))
	defer target.Close()
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "cli-timeout-retry", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 1, MaxRetries: 1, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "retry-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	triggerOutput := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "timeout-retry")
	var initial struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(triggerOutput, &initial); err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "retry-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	fixture.useEngine(engine)
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(7 * time.Second)
	var retry store.Run
	for {
		runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		for _, run := range runs {
			if run.TriggerType == "retry" && run.Status == "succeeded" {
				retry = run
			}
		}
		if retry.ID != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry did not succeed: %+v", runs)
		}
		time.Sleep(25 * time.Millisecond)
	}
	first, err := fixture.store.GetRun(t.Context(), fixture.tenantID, initial.ID)
	if err != nil || (first.Status != "timed_out" && first.Status != "failed") || !strings.Contains(first.ErrorMessage, "deadline exceeded") {
		t.Fatalf("first attempt = %+v, %v", first, err)
	}
	getOutput := runCLI("runs", "get", retry.ID)
	if !bytes.Contains(getOutput, []byte(initial.ID)) || !bytes.Contains(getOutput, []byte(`"attempt": 2`)) {
		t.Fatalf("retry lineage output = %s", getOutput)
	}
}

func TestExecutorRoutingStrategiesUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var nodeARequests atomic.Int32
	var nodeBRequests atomic.Int32
	routingHandlers := func(counter *atomic.Int32) map[string]executorsdk.Handler {
		handlers := map[string]executorsdk.Handler{}
		for _, name := range []string{"billing.sync", "first", "last", "random", "hash", "lfu", "lru"} {
			handlers[name] = func(_ context.Context, task executorsdk.Task) error {
				if task.RunID == "" || task.HTTP == nil && task.Input == "" {
					t.Errorf("executor task = %+v", task)
				}
				counter.Add(1)
				return nil
			}
		}
		return handlers
	}
	nodeA := startTestGRPCExecutor(t, fixture, routingHandlers(&nodeARequests))
	nodeB := startTestGRPCExecutor(t, fixture, routingHandlers(&nodeBRequests))
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "executor-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupOutput := runCLI("executors", "groups", "create", "--name", "billing-workers", "--strategy", "round")
	var group struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(groupOutput, &group); err != nil || group.ID == "" {
		t.Fatalf("group output = %s, %v", groupOutput, err)
	}
	runCLI("executors", "register", group.ID, "node-a", "--address", nodeA, "--ttl", "30")
	runCLI("executors", "register", group.ID, "node-b", "--address", nodeB, "--ttl", "30")
	nodesOutput := runCLI("executors", "list", group.ID)
	if !bytes.Contains(nodesOutput, []byte("node-a")) || !bytes.Contains(nodesOutput, []byte("node-b")) {
		t.Fatalf("nodes output = %s", nodesOutput)
	}
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "executor-round", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: group.ID, ExecutorHandler: "billing.sync", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	runCLI("jobs", "trigger", job.ID, "--idempotency-key", "route-a", "--input", "first")
	runCLI("jobs", "trigger", job.ID, "--idempotency-key", "route-b", "--input", "second")
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "routing-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		succeeded := 0
		for _, run := range runs {
			if run.Status == "succeeded" {
				succeeded++
			}
		}
		if succeeded == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("routed runs did not complete: %+v", runs)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if nodeARequests.Load() != 1 || nodeBRequests.Load() != 1 {
		t.Fatalf("round distribution: node-a=%d node-b=%d", nodeARequests.Load(), nodeBRequests.Load())
	}
	runsOutput := runCLI("runs", "--job", job.ID)
	if !bytes.Contains(runsOutput, []byte("node-a")) || !bytes.Contains(runsOutput, []byte("node-b")) {
		t.Fatalf("runs omitted executor nodes: %s", runsOutput)
	}
	runStrategy := func(strategy string) []store.Run {
		t.Helper()
		output := runCLI("executors", "groups", "create", "--name", "workers-"+strategy, "--strategy", strategy)
		var strategyGroup struct {
			ID string `json:"id"`
		}
		if unmarshalErr := json.Unmarshal(output, &strategyGroup); unmarshalErr != nil || strategyGroup.ID == "" {
			t.Fatalf("%s group output = %s, %v", strategy, output, unmarshalErr)
		}
		runCLI("executors", "register", strategyGroup.ID, "node-a", "--address", nodeA, "--ttl", "30")
		runCLI("executors", "register", strategyGroup.ID, "node-b", "--address", nodeB, "--ttl", "30")
		strategyJob, createErr := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "executor-" + strategy, ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: strategyGroup.ID, ExecutorHandler: strategy, Enabled: false})
		if createErr != nil {
			t.Fatal(createErr)
		}
		runCLI("jobs", "trigger", strategyJob.ID, "--idempotency-key", strategy+"-1", "--input", "first")
		runCLI("jobs", "trigger", strategyJob.ID, "--idempotency-key", strategy+"-2", "--input", "second")
		strategyDeadline := time.Now().Add(5 * time.Second)
		for {
			runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, strategyJob.ID, 10)
			if listErr != nil {
				t.Fatal(listErr)
			}
			succeeded := 0
			for _, run := range runs {
				if run.Status == "succeeded" {
					succeeded++
				}
			}
			if succeeded == 2 {
				return runs
			}
			if time.Now().After(strategyDeadline) {
				t.Fatalf("%s runs did not complete: %+v", strategy, runs)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	for _, strategy := range []string{"first", "last", "random", "hash", "lfu", "lru"} {
		beforeA, beforeB := nodeARequests.Load(), nodeBRequests.Load()
		runs := runStrategy(strategy)
		deltaA, deltaB := nodeARequests.Load()-beforeA, nodeBRequests.Load()-beforeB
		switch strategy {
		case "first":
			if deltaA != 2 || deltaB != 0 {
				t.Fatalf("first distribution = %d/%d", deltaA, deltaB)
			}
		case "last":
			if deltaA != 0 || deltaB != 2 {
				t.Fatalf("last distribution = %d/%d", deltaA, deltaB)
			}
		case "random":
			if deltaA+deltaB != 2 {
				t.Fatalf("random distribution = %d/%d", deltaA, deltaB)
			}
		case "hash":
			if len(runs) != 2 || runs[0].ExecutorNodeID == "" || runs[0].ExecutorNodeID != runs[1].ExecutorNodeID {
				t.Fatalf("hash did not keep job affinity: %+v", runs)
			}
		case "lfu", "lru":
			if deltaA != 1 || deltaB != 1 {
				t.Fatalf("%s distribution = %d/%d", strategy, deltaA, deltaB)
			}
		}
	}
}

func TestExecutorRoutingDatabaseWaitHonorsRunTimeout(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	group, err := fixture.store.CreateExecutorGroup(t.Context(), store.ExecutorGroup{
		TenantID:         fixture.tenantID,
		Name:             "timeout-workers",
		RouteStrategy:    "round",
		RegistrationMode: "manual",
		ManualAddresses:  []string{"http://worker-a.invalid", "http://worker-b.invalid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := fixture.store.CreateJob(t.Context(), store.Job{
		TenantID: fixture.tenantID, Name: "routing-timeout", ScheduleType: "fixed_interval",
		ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: http.MethodPost,
		Headers: map[string]string{}, TimeoutSeconds: 1, OverlapPolicy: "serial",
		MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: group.ID,
		ExecutorHandler: "timeout", Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "routing-timeout", "")
	if err != nil {
		t.Fatal(err)
	}

	connection, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	if _, err = connection.Exec(t.Context(), `INSERT INTO executor_job_route_counters(job_id,route_count) VALUES($1,0)`, job.ID); err != nil {
		t.Fatal(err)
	}
	lock, err := connection.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM executor_job_route_counters WHERE job_id=$1 FOR UPDATE`, job.ID); err != nil {
		t.Fatal(err)
	}

	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "routing-timeout-core", 20*time.Millisecond, 1, "http://api.invalid", 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() {
		cancelEngine()
		engine.Wait()
	}()

	deadline := time.Now().Add(4 * time.Second)
	for {
		current, getErr := fixture.store.GetRun(t.Context(), fixture.tenantID, run.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.Status == "timed_out" {
			break
		}
		if current.Status != "pending" && current.Status != "running" {
			t.Fatalf("run ended as %s, want timed_out: %+v", current.Status, current)
		}
		if time.Now().After(deadline) {
			t.Fatalf("routing database wait ignored run timeout: %+v", current)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestTriggerAddressOverrideUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var firstCalls, secondCalls atomic.Int32
	first := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"override.handler": countingHandler(&firstCalls)})
	second := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"override.handler": countingHandler(&secondCalls)})
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "override-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupOutput := runCLI("executors", "groups", "create", "--name", "override-e2e", "--strategy", "round")
	var group struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(groupOutput, &group); err != nil || group.ID == "" {
		t.Fatalf("group=%s err=%v", groupOutput, err)
	}
	runCLI("executors", "register", group.ID, "broken-a", "--address", "http://127.0.0.1:1", "--ttl", "30")
	runCLI("executors", "register", group.ID, "broken-b", "--address", "http://127.0.0.1:2", "--ttl", "30")
	runCLI("executors", "register", group.ID, "override-a", "--address", first, "--ttl", "30")
	runCLI("executors", "register", group.ID, "override-b", "--address", second, "--ttl", "30")
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "override-e2e-job", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxQueueSize: 10, ExecutorGroupID: group.ID, ExecutorHandler: "override.handler", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"override-a", "override-b"} {
		runCLI("jobs", "trigger", job.ID, "--idempotency-key", key, "--address", first, "--address", second)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "override-e2e-core", 10*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for firstCalls.Load()+secondCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("override ROUND calls first=%d second=%d", firstCalls.Load(), secondCalls.Load())
	}
	runsOutput := runCLI("runs", "--job", job.ID)
	if !bytes.Contains(runsOutput, []byte(first)) || !bytes.Contains(runsOutput, []byte(second)) || !bytes.Contains(runsOutput, []byte("override_addresses")) || bytes.Contains(runsOutput, []byte("127.0.0.1:1")) {
		t.Fatalf("override runs=%s", runsOutput)
	}
}

func TestExecutorActiveRoutingUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var rejectedRuns, acceptedRuns atomic.Int32
	first := startProbeLifecycleExecutor(t, fixture, false, true, &rejectedRuns)
	second := startProbeLifecycleExecutor(t, fixture, true, false, &acceptedRuns)
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "active-routing-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	jobs := make([]store.Job, 0, 2)
	for _, strategy := range []string{"failover", "busyover"} {
		output := runCLI("executors", "groups", "create", "--name", "active-"+strategy, "--strategy", strategy)
		var group struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(output, &group); err != nil {
			t.Fatal(err)
		}
		runCLI("executors", "register", group.ID, "node-a", "--address", first, "--ttl", "30")
		runCLI("executors", "register", group.ID, "node-b", "--address", second, "--ttl", "30")
		job, createErr := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "active-" + strategy, ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: group.ID, ExecutorHandler: strategy, Enabled: false})
		if createErr != nil {
			t.Fatal(createErr)
		}
		jobs = append(jobs, job)
		runCLI("jobs", "trigger", job.ID, "--idempotency-key", strategy, "--input", strategy)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "active-routing-core", 20*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		complete := true
		for _, job := range jobs {
			runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
			if listErr != nil {
				t.Fatal(listErr)
			}
			if len(runs) != 1 || runs[0].Status != "succeeded" || runs[0].ExecutorNodeID != "node-b" {
				complete = false
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("active routes did not complete")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if rejectedRuns.Load() != 0 || acceptedRuns.Load() != 2 {
		t.Fatalf("run calls rejected=%d accepted=%d", rejectedRuns.Load(), acceptedRuns.Load())
	}
}

func TestKubernetesExecutorLabelRoutingUseCase(t *testing.T) {
	fixture := newEncryptedLifecycleFixture(t)
	defer fixture.close()
	var kubernetesCalls, excludedCalls atomic.Int32
	var dispatched struct {
		ExternalExecutionID string
		Cluster             *executorsdk.KubernetesClusterConfig
	}
	kubernetesNode := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"__kubernetes__": func(_ context.Context, task executorsdk.Task) error {
		kubernetesCalls.Add(1)
		dispatched.ExternalExecutionID = task.ExternalExecutionID
		dispatched.Cluster = task.KubernetesCluster
		return nil
	}})
	excludedNode := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"__kubernetes__": countingHandler(&excludedCalls)})
	group, err := fixture.store.CreateExecutorGroup(t.Context(), store.ExecutorGroup{TenantID: fixture.tenantID, Name: "label-routing", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.RegisterExecutorNode(t.Context(), fixture.tenantID, group.ID, "kubernetes-node", kubernetesNode, 30*time.Second, []string{"kubernetes", "linux"}); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.RegisterExecutorNode(t.Context(), fixture.tenantID, group.ID, "excluded-node", excludedNode, 30*time.Second, []string{"kubernetes", "linux", "spot"}); err != nil {
		t.Fatal(err)
	}
	cluster, err := fixture.store.CreateKubernetesCluster(t.Context(), store.KubernetesCluster{TenantID: fixture.tenantID, Name: "routing-cluster", AuthMode: "service_account", APIServer: "https://k8s.example", Namespace: "jobs", Credentials: store.KubernetesCredentials{Token: "routing-token"}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "kubernetes-label-routed", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, CallbackTimeoutSeconds: 30, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, ExecutorGroupID: group.ID, ExecutorHandler: "__kubernetes__", ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22"}`, KubernetesClusterID: cluster.ID, Enabled: false, RequiredExecutorLabels: []string{"kubernetes", "linux"}, ExcludedExecutorLabels: []string{"spot"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "labels-match", ""); err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "label-routing-core", 10*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	fixture.useEngine(engine)
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	var runs []store.Run
	for {
		var listErr error
		runs, listErr = fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(runs) == 1 && runs[0].Status == "succeeded" && runs[0].ExecutorNodeID == "kubernetes-node" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("label routing calls kubernetes=%d excluded=%d runs=%+v", kubernetesCalls.Load(), excludedCalls.Load(), runs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if kubernetesCalls.Load() != 1 || excludedCalls.Load() != 0 {
		t.Fatalf("label routing calls kubernetes=%d excluded=%d", kubernetesCalls.Load(), excludedCalls.Load())
	}
	if dispatched.ExternalExecutionID != runs[0].ID || dispatched.Cluster == nil || dispatched.Cluster.Token != "routing-token" || dispatched.Cluster.Namespace != "jobs" {
		t.Fatalf("kubernetes dispatch = %+v", dispatched)
	}
	job.RequiredExecutorLabels = []string{"windows"}
	job.ExcludedExecutorLabels = nil
	job, err = fixture.store.UpdateJob(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "labels-no-match", ""); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, err = fixture.store.ListRuns(t.Context(), fixture.tenantID, job.ID, 10)
		if err == nil && len(runs) == 2 && runs[0].Status == "failed" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(runs) != 2 || runs[0].Status != "failed" || !strings.Contains(runs[0].ErrorMessage, "no live executor nodes") {
		t.Fatalf("no-match runs = %+v", runs)
	}
	if kubernetesCalls.Load() != 1 || excludedCalls.Load() != 0 {
		t.Fatalf("no-match dispatched unexpectedly kubernetes=%d excluded=%d", kubernetesCalls.Load(), excludedCalls.Load())
	}
}

func TestExecutorRetryUsesDistinctExternalExecutionIDUseCase(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	executionIDs := make(chan string, 2)
	node := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"retry": func(_ context.Context, task executorsdk.Task) error {
		executionIDs <- task.ExternalExecutionID
		return errors.New("force retry")
	}})

	group, err := fixture.store.CreateExecutorGroup(t.Context(), store.ExecutorGroup{TenantID: fixture.tenantID, Name: "retry-execution-id", RouteStrategy: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.RegisterExecutorNode(t.Context(), fixture.tenantID, group.ID, "retry-node", node, 30*time.Second, []string{}); err != nil {
		t.Fatal(err)
	}
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "distinct-retry-execution", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, MaxRetries: 1, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: group.ID, ExecutorHandler: "retry", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, "distinct-retry-execution", ""); err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "retry-execution-core", 10*time.Millisecond, 1, "http://scheduler.test", 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()

	seen := make([]string, 0, 2)
	for len(seen) < 2 {
		select {
		case executionID := <-executionIDs:
			if executionID == "" {
				t.Fatal("executor received an empty external execution ID")
			}
			seen = append(seen, executionID)
		case <-time.After(5 * time.Second):
			t.Fatalf("executor retry IDs = %+v", seen)
		}
	}
	if seen[0] == seen[1] {
		t.Fatalf("executor retry reused completed external execution ID: %+v", seen)
	}
}

func TestShardingBroadcastUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var nodeACalls, nodeBCalls atomic.Int32
	type dispatch struct {
		GroupID string
		Index   int32
		Total   int32
	}
	var nodeADispatch, nodeBDispatch dispatch
	nodeA := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"broadcast": func(_ context.Context, task executorsdk.Task) error {
		nodeADispatch = dispatch{GroupID: task.BroadcastGroupID, Index: task.BroadcastIndex, Total: task.BroadcastTotal}
		if nodeACalls.Add(1) == 1 {
			return errors.New("broadcast shard failed")
		}
		return nil
	}})
	nodeB := startTestGRPCExecutor(t, fixture, map[string]executorsdk.Handler{"broadcast": func(_ context.Context, task executorsdk.Task) error {
		nodeBDispatch = dispatch{GroupID: task.BroadcastGroupID, Index: task.BroadcastIndex, Total: task.BroadcastTotal}
		if logErr := task.Logger.Info("node-b started"); logErr != nil {
			return logErr
		}
		nodeBCalls.Add(1)
		return nil
	}})
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "broadcast-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupOutput := runCLI("executors", "groups", "create", "--name", "broadcast", "--strategy", "sharding_broadcast")
	var group struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(groupOutput, &group); err != nil {
		t.Fatal(err)
	}
	runCLI("executors", "register", group.ID, "node-b", "--address", nodeB, "--ttl", "30")
	runCLI("executors", "register", group.ID, "node-a", "--address", nodeA, "--ttl", "30")
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "broadcast-e2e", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: group.ID, ExecutorHandler: "broadcast", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	triggerOutput := runCLI("jobs", "trigger", job.ID, "--idempotency-key", "broadcast-e2e")
	var primary struct {
		BroadcastGroupID string `json:"broadcast_group_id"`
	}
	if err = json.Unmarshal(triggerOutput, &primary); err != nil || primary.BroadcastGroupID == "" {
		t.Fatalf("trigger output = %s, %v", triggerOutput, err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "broadcast-core", 20*time.Millisecond, 3, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(8 * time.Second)
	for nodeACalls.Load() < 2 || nodeBCalls.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("broadcast calls node-a=%d node-b=%d", nodeACalls.Load(), nodeBCalls.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	runsOutput := runCLI("runs", "--broadcast-group", primary.BroadcastGroupID)
	var listed struct {
		Runs []struct {
			ID               string `json:"id"`
			BroadcastGroupID string `json:"broadcast_group_id"`
			ShardIndex       int32  `json:"shard_index"`
		} `json:"runs"`
	}
	if err = json.Unmarshal(runsOutput, &listed); err != nil || len(listed.Runs) != 3 {
		t.Fatalf("runs output = %s, %v", runsOutput, err)
	}
	if nodeADispatch.GroupID != primary.BroadcastGroupID || nodeADispatch.Index != 0 || nodeADispatch.Total != 2 || nodeBDispatch.Index != 1 || nodeBDispatch.Total != 2 {
		t.Fatalf("dispatches node-a=%+v node-b=%+v", nodeADispatch, nodeBDispatch)
	}
	var nodeBRunID string
	for _, run := range listed.Runs {
		if run.ShardIndex == 1 && run.BroadcastGroupID == primary.BroadcastGroupID {
			nodeBRunID = run.ID
		}
	}
	if nodeBRunID == "" {
		t.Fatal("node-b run was not listed")
	}
	logsOutput := runCLI("runs", "logs", nodeBRunID)
	if !bytes.Contains(logsOutput, []byte("node-b started")) {
		t.Fatalf("logs output = %s", logsOutput)
	}
}

func TestFixedDelayUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	starts := make(chan time.Time, 3)
	finishes := make(chan time.Time, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		starts <- time.Now()
		time.Sleep(250 * time.Millisecond)
		finishes <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "fixed-delay-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupID := attachHTTPExecutor(t, fixture)
	definition, err := json.Marshal(map[string]any{"name": "fixed-delay-e2e", "schedule_type": "fixed_delay", "schedule_expression": "1", "timezone": "UTC", "target_url": target.URL, "http_method": "POST", "timeout_seconds": 5, "overlap_policy": "parallel", "misfire_policy": "fire_once", "max_concurrent_runs": 1, "max_queue_size": 10, "enabled": true, "executor_group_id": groupID})
	if err != nil {
		t.Fatal(err)
	}
	createdOutput := runCLI(definition, "jobs", "create", "--file", "-")
	var created struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(createdOutput, &created); err != nil || created.ID == "" {
		t.Fatalf("created job = %s, %v", createdOutput, err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "fixed-delay-core", 20*time.Millisecond, 2, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	var firstStart time.Time
	select {
	case firstStart = <-starts:
	case <-time.After(3 * time.Second):
		t.Fatal("first fixed-delay execution did not start")
	}
	pendingOutput := runCLI(nil, "jobs", "get", created.ID)
	var pending map[string]any
	if err = json.Unmarshal(pendingOutput, &pending); err != nil {
		t.Fatal(err)
	}
	if next, exists := pending["next_run_at"]; exists && next != nil && next != "" {
		t.Fatalf("fixed delay exposed next_run_at while running: %v", next)
	}
	var firstFinish time.Time
	select {
	case firstFinish = <-finishes:
	case <-time.After(time.Second):
		t.Fatal("first fixed-delay execution did not finish")
	}
	select {
	case secondStart := <-starts:
		if secondStart.Sub(firstFinish) < 950*time.Millisecond {
			t.Fatalf("fixed delay measured from start: firstStart=%s firstFinish=%s secondStart=%s", firstStart, firstFinish, secondStart)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second fixed-delay execution did not start")
	}
	runsOutput := runCLI(nil, "runs", "--job", created.ID)
	if !bytes.Contains(runsOutput, []byte(`"trigger_type": "schedule"`)) {
		t.Fatalf("runs output = %s", runsOutput)
	}
}

func TestCronSchedulingUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var regularCalls, quartzCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/quartz" {
			quartzCalls.Add(1)
		} else {
			regularCalls.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "cron-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupID := attachHTTPExecutor(t, fixture)
	definition, err := json.Marshal(map[string]any{"name": "cron-e2e", "schedule_type": "cron", "schedule_expression": "0/1 * * * * ?", "timezone": "Asia/Shanghai", "target_url": target.URL, "http_method": "POST", "timeout_seconds": 5, "overlap_policy": "parallel", "misfire_policy": "fire_once", "max_concurrent_runs": 1, "max_catch_up": 10, "max_queue_size": 10, "enabled": true, "executor_group_id": groupID})
	if err != nil {
		t.Fatal(err)
	}
	createdOutput := runCLI(definition, "jobs", "create", "--file", "-")
	var created struct {
		ID        string `json:"id"`
		Timezone  string `json:"timezone"`
		NextRunAt string `json:"next_run_at"`
	}
	if err = json.Unmarshal(createdOutput, &created); err != nil || created.ID == "" || created.NextRunAt == "" || created.Timezone != "Asia/Shanghai" {
		t.Fatalf("created cron = %s, %v", createdOutput, err)
	}
	quartzDefinition, err := json.Marshal(map[string]any{"name": "quartz-cron-e2e", "schedule_type": "cron", "schedule_expression": "0 0 9 L * ?", "timezone": "UTC", "target_url": target.URL + "/quartz", "http_method": "POST", "timeout_seconds": 5, "overlap_policy": "parallel", "misfire_policy": "fire_once", "max_concurrent_runs": 1, "max_catch_up": 10, "max_queue_size": 10, "enabled": true, "executor_group_id": groupID})
	if err != nil {
		t.Fatal(err)
	}
	quartzOutput := runCLI(quartzDefinition, "jobs", "create", "--file", "-")
	var quartzCreated struct {
		ID        string `json:"id"`
		NextRunAt string `json:"next_run_at"`
	}
	if err = json.Unmarshal(quartzOutput, &quartzCreated); err != nil || quartzCreated.ID == "" || quartzCreated.NextRunAt == "" {
		t.Fatalf("created Quartz cron = %s, %v", quartzOutput, err)
	}
	adminConn, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer adminConn.Close(t.Context())
	if _, err = adminConn.Exec(t.Context(), `UPDATE jobs SET next_run_at=now()-interval '5 seconds' WHERE id=$1`, quartzCreated.ID); err != nil {
		t.Fatal(err)
	}
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "cron-core", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for regularCalls.Load() < 1 || quartzCalls.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("cron targets regular=%d quartz=%d", regularCalls.Load(), quartzCalls.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	runsOutput := runCLI(nil, "runs", "--job", created.ID)
	if !bytes.Contains(runsOutput, []byte(`"trigger_type": "schedule"`)) || !bytes.Contains(runsOutput, []byte(`"status": "succeeded"`)) {
		t.Fatalf("cron runs = %s", runsOutput)
	}
	jobOutput := runCLI(nil, "jobs", "get", created.ID)
	var advanced struct {
		NextRunAt string `json:"next_run_at"`
	}
	if err = json.Unmarshal(jobOutput, &advanced); err != nil || advanced.NextRunAt == "" || advanced.NextRunAt == created.NextRunAt {
		t.Fatalf("cron next_run_at did not advance: created=%s current=%s err=%v", created.NextRunAt, advanced.NextRunAt, err)
	}
	quartzRuns := runCLI(nil, "runs", "--job", quartzCreated.ID)
	if !bytes.Contains(quartzRuns, []byte(`"trigger_type": "schedule"`)) || !bytes.Contains(quartzRuns, []byte(`"status": "succeeded"`)) {
		t.Fatalf("Quartz runs = %s", quartzRuns)
	}
	quartzJobOutput := runCLI(nil, "jobs", "get", quartzCreated.ID)
	var quartzAdvanced struct {
		NextRunAt string `json:"next_run_at"`
	}
	if err = json.Unmarshal(quartzJobOutput, &quartzAdvanced); err != nil {
		t.Fatal(err)
	}
	quartzNext, err := time.Parse(time.RFC3339, quartzAdvanced.NextRunAt)
	if err != nil {
		t.Fatalf("Quartz next_run_at=%q: %v", quartzAdvanced.NextRunAt, err)
	}
	if quartzNext.Day() != time.Date(quartzNext.Year(), quartzNext.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day() || quartzNext.Hour() != 9 {
		t.Fatalf("Quartz next_run_at=%s, want month end 09:00 UTC", quartzNext)
	}
}

func TestWorkerSaturationDoesNotBlockSchedulerUseCase(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()

	blockingStarted := make(chan struct{}, 1)
	releaseBlocking := make(chan struct{})
	var released bool
	defer func() {
		if !released {
			close(releaseBlocking)
		}
	}()
	scheduledExecuted := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/blocking":
			select {
			case blockingStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseBlocking:
			case <-r.Context().Done():
			}
		case "/scheduled":
			select {
			case scheduledExecuted <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	groupID := attachHTTPExecutor(t, fixture)
	blockingJob, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "saturated-worker", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL + "/blocking", HTTPMethod: http.MethodPost, Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	scheduledJob, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "scheduled-while-saturated", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL + "/scheduled", HTTPMethod: http.MethodPost, Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: true, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.TriggerJob(t.Context(), fixture.tenantID, blockingJob.ID, "saturation-blocker", ""); err != nil {
		t.Fatal(err)
	}

	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "saturation-core", 20*time.Millisecond, 1, target.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() {
		cancelEngine()
		engine.Wait()
	}()
	select {
	case <-blockingStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("blocking run did not occupy the worker")
	}

	connection, err := pgx.Connect(t.Context(), fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Exec(t.Context(), `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, scheduledJob.ID); err != nil {
		_ = connection.Close(t.Context())
		t.Fatal(err)
	}
	if err = connection.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		runs, listErr := fixture.store.ListRuns(t.Context(), fixture.tenantID, scheduledJob.ID, 10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(runs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("due run was not enqueued while executor HTTP was in flight")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-scheduledExecuted:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduled run did not execute while another HTTP job was in flight")
	}
	close(releaseBlocking)
	released = true
}

func TestWorkerCompletionImmediatelyDispatchesPendingRunUseCase(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-r.Context().Done():
			}
		} else {
			close(secondStarted)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "wake-dispatcher", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: target.URL, HTTPMethod: http.MethodPost, Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"wake-first", "wake-second"} {
		if _, err = fixture.store.TriggerJob(t.Context(), fixture.tenantID, job.ID, key, ""); err != nil {
			t.Fatal(err)
		}
	}

	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "wake-dispatch-core", 5*time.Second, 1, target.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	fixture.useEngine(engine)
	engine.Run(engineCtx)
	defer func() {
		cancelEngine()
		engine.Wait()
	}()

	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first pending run was not dispatched")
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second run waited for the five-second scheduler tick")
	}
}

func TestMisfireRecoveryUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var skipCalls, fireOnceCalls, catchUpCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/skip":
			skipCalls.Add(1)
		case "/fire_once":
			fireOnceCalls.Add(1)
		case "/catch_up":
			catchUpCalls.Add(1)
		default:
			t.Errorf("unexpected target path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "misfire-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(input []byte, args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		command.Stdin = bytes.NewReader(input)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	groupID := attachHTTPExecutor(t, fixture)
	jobIDs := make(map[string]string, 3)
	for _, policy := range []string{"skip", "fire_once", "catch_up"} {
		definition, marshalErr := json.Marshal(map[string]any{"name": "misfire-e2e-" + policy, "schedule_type": "fixed_rate", "schedule_expression": "1", "timezone": "UTC", "target_url": target.URL + "/" + policy, "http_method": "POST", "timeout_seconds": 5, "overlap_policy": "parallel", "misfire_policy": policy, "max_concurrent_runs": 4, "max_catch_up": 3, "max_queue_size": 10, "enabled": true, "executor_group_id": groupID})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		createdOutput := runCLI(definition, "jobs", "create", "--file", "-")
		var created struct {
			ID string `json:"id"`
		}
		if err = json.Unmarshal(createdOutput, &created); err != nil || created.ID == "" {
			t.Fatalf("created %s = %s, %v", policy, createdOutput, err)
		}
		jobIDs[policy] = created.ID
	}
	time.Sleep(3500 * time.Millisecond)
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "misfire-core", 20*time.Millisecond, 4, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for fireOnceCalls.Load() < 1 || catchUpCalls.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("misfire target calls skip=%d fire_once=%d catch_up=%d", skipCalls.Load(), fireOnceCalls.Load(), catchUpCalls.Load())
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancelEngine()
	engine.Wait()
	if skipCalls.Load() != 0 || fireOnceCalls.Load() != 1 || catchUpCalls.Load() != 3 {
		t.Fatalf("misfire target calls skip=%d fire_once=%d catch_up=%d", skipCalls.Load(), fireOnceCalls.Load(), catchUpCalls.Load())
	}
	for policy, want := range map[string]int{"skip": 0, "fire_once": 1, "catch_up": 3} {
		runsOutput := runCLI(nil, "runs", "--job", jobIDs[policy])
		var listed struct {
			Runs []json.RawMessage `json:"runs"`
		}
		if err = json.Unmarshal(runsOutput, &listed); err != nil || len(listed.Runs) != want {
			t.Fatalf("misfire %s runs=%s want=%d err=%v", policy, runsOutput, want, err)
		}
	}
}

func TestFailureNotificationUseCaseThroughCLI(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	var stableCalls, flakyCalls atomic.Int32
	stable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stableCalls.Add(1)
		var event struct {
			Topic string `json:"topic"`
		}
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil || event.Topic != "job.run.exhausted" {
			t.Errorf("stable event = %+v, %v", event, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer stable.Close()
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if flakyCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer flaky.Close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "notifications-e2e", "developer")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	malformedRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/api/v1/notification-channels", strings.NewReader(`{"kind":"webhook","name":"must-not-exist","config":{"url":"https://alerts.invalid"},"events":["exhausted"],"all_jobs":true,"max_attempts":1,"backoff_initial_seconds":1,"backoff_max_seconds":1}{"name":"trailing"}`))
	if err != nil {
		t.Fatal(err)
	}
	malformedRequest.Header.Set("Authorization", "Bearer "+token)
	malformedRequest.Header.Set("Content-Type", "application/json")
	malformedResponse, err := httpServer.Client().Do(malformedRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = malformedResponse.Body.Close()
	if malformedResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing notification request status = %d, want %d", malformedResponse.StatusCode, http.StatusBadRequest)
	}
	if channels, listErr := fixture.store.NotificationChannels(t.Context(), fixture.tenantID); listErr != nil || len(channels) != 0 {
		t.Fatalf("trailing notification request mutated channels: %+v, %v", channels, listErr)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "schedulerctl")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/schedulerctl")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		t.Fatalf("build schedulerctl: %v\n%s", buildErr, output)
	}
	runCLI := func(args ...string) []byte {
		command := exec.CommandContext(t.Context(), binary, append([]string{"--server", httpServer.URL, "--token", token}, args...)...)
		output, commandErr := command.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("schedulerctl %v: %v\n%s", args, commandErr, output)
		}
		return output
	}
	runCLI("notifications", "create", "--kind", "webhook", "--name", "stable", "--config", `{"url":"`+stable.URL+`"}`)
	runCLI("notifications", "create", "--kind", "webhook", "--name", "flaky", "--config", `{"url":"`+flaky.URL+`"}`)
	listed := runCLI("notifications", "list")
	if !bytes.Contains(listed, []byte("stable")) || !bytes.Contains(listed, []byte("flaky")) {
		t.Fatalf("notification list = %s", listed)
	}
	groupID := attachHTTPExecutor(t, fixture)
	job, err := fixture.store.CreateJob(t.Context(), store.Job{TenantID: fixture.tenantID, Name: "notification-failure", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "http://127.0.0.1:1/unavailable", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 1, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false, ExecutorGroupID: groupID, ExecutorHandler: "__http__"})
	if err != nil {
		t.Fatal(err)
	}
	runCLI("jobs", "trigger", job.ID, "--idempotency-key", "notification-failure")
	engineCtx, cancelEngine := context.WithCancel(t.Context())
	engine := core.NewEngine(fixture.store, "notification-engine", 20*time.Millisecond, 1, httpServer.URL, 90*24*time.Hour, nil, core.WithExecutorGRPC("lifecycle-executor-token"))
	engine.Run(engineCtx)
	defer func() { cancelEngine(); engine.Wait() }()
	notifierCtx, cancelNotifier := context.WithCancel(t.Context())
	notifications := notifier.New(fixture.store, "notification-worker", notifier.SMTPConfig{})
	notifications.Run(notifierCtx)
	defer func() { cancelNotifier(); notifications.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for stableCalls.Load() < 1 || flakyCalls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("notification calls stable=%d flaky=%d", stableCalls.Load(), flakyCalls.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if stableCalls.Load() != 1 || flakyCalls.Load() != 2 {
		t.Fatalf("duplicate notification calls stable=%d flaky=%d", stableCalls.Load(), flakyCalls.Load())
	}
	type notificationHistoryPage struct {
		Deliveries []struct {
			ChannelName string `json:"channel_name"`
			JobID       string `json:"job_id"`
			Status      string `json:"status"`
		} `json:"deliveries"`
		NextCursor string `json:"next_cursor"`
	}
	readHistoryPage := func(args ...string) notificationHistoryPage {
		var page notificationHistoryPage
		if unmarshalErr := json.Unmarshal(runCLI(args...), &page); unmarshalErr != nil {
			t.Fatalf("decode schedulerctl notification history: %v", unmarshalErr)
		}
		return page
	}
	firstPage := readHistoryPage("notifications", "history", "--job-id", job.ID, "--status", "delivered", "--limit", "1")
	if len(firstPage.Deliveries) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("first schedulerctl notification history page = %+v", firstPage)
	}
	secondPage := readHistoryPage("notifications", "history", "--job-id", job.ID, "--status", "delivered", "--limit", "1", "--cursor", firstPage.NextCursor)
	if len(secondPage.Deliveries) != 1 {
		t.Fatalf("second schedulerctl notification history page = %+v", secondPage)
	}
	channels := map[string]bool{
		firstPage.Deliveries[0].ChannelName:  true,
		secondPage.Deliveries[0].ChannelName: true,
	}
	for _, page := range []notificationHistoryPage{firstPage, secondPage} {
		if page.Deliveries[0].JobID != job.ID || page.Deliveries[0].Status != "delivered" {
			t.Fatalf("schedulerctl notification history entry = %+v", page.Deliveries[0])
		}
	}
	if !channels["stable"] || !channels["flaky"] {
		t.Fatalf("schedulerctl notification history channels = %+v", channels)
	}
}

func TestKubernetesClusterUpdatePreservesCredentials(t *testing.T) {
	fixture := newLifecycleFixture(t)
	defer fixture.close()
	_, token, err := fixture.store.CreateAPIKey(t.Context(), fixture.tenantID, "kubernetes-update-e2e", "admin")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := auth.NewManager(string(bytes.Repeat([]byte("x"), 32)), "test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(apihttp.NewServer(fixture.client, manager, false).Routes())
	defer httpServer.Close()
	request := func(method, path, body string) (*http.Response, []byte) {
		req, requestErr := http.NewRequestWithContext(t.Context(), method, httpServer.URL+path, strings.NewReader(body))
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, requestErr := httpServer.Client().Do(req)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		defer response.Body.Close()
		raw, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		return response, raw
	}
	createdResponse, createdBody := request(http.MethodPost, "/api/v1/kubernetes-clusters", `{"name":"preserved","auth_mode":"service_account","api_server":"https://kubernetes.example","namespace":"jobs","token":"secret-token","ca_data":"secret-ca","max_concurrent_jobs":10}`)
	if createdResponse.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", createdResponse.StatusCode, createdBody)
	}
	var created struct {
		ID      string `json:"id"`
		Version int64  `json:"version"`
	}
	if err = json.Unmarshal(createdBody, &created); err != nil || created.ID == "" || created.Version != 1 {
		t.Fatalf("created cluster=%s err=%v", createdBody, err)
	}
	updatedResponse, updatedBody := request(http.MethodPut, "/api/v1/kubernetes-clusters/"+created.ID, `{"name":"preserved-updated","auth_mode":"service_account","api_server":"https://kubernetes.example","namespace":"jobs-updated","max_concurrent_jobs":25,"version":1}`)
	if updatedResponse.StatusCode != http.StatusOK {
		t.Fatalf("metadata update status=%d body=%s", updatedResponse.StatusCode, updatedBody)
	}
	stored, err := fixture.store.GetKubernetesCluster(t.Context(), fixture.tenantID, created.ID)
	if err != nil || stored.Credentials.Token != "secret-token" || stored.Credentials.CAData != "secret-ca" || stored.MaxConcurrentJobs != 25 {
		t.Fatalf("stored cluster=%+v err=%v", stored, err)
	}
	changedResponse, changedBody := request(http.MethodPut, "/api/v1/kubernetes-clusters/"+created.ID, `{"name":"changed-auth","auth_mode":"kubeconfig","namespace":"jobs","max_concurrent_jobs":25,"version":2}`)
	if changedResponse.StatusCode != http.StatusBadRequest || !bytes.Contains(changedBody, []byte("credentials are required")) {
		t.Fatalf("auth-mode update status=%d body=%s", changedResponse.StatusCode, changedBody)
	}
}
