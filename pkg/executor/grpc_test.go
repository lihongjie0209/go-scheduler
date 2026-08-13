package executor

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type recordingReporter struct {
	mu        sync.Mutex
	completed chan bool
}

func (r *recordingReporter) AppendLog(context.Context, string, string, string, string) error {
	return nil
}
func (r *recordingReporter) Complete(_ context.Context, _ string, _ string, succeeded bool, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed <- succeeded
	return nil
}

func TestGRPCDispatchIsAsyncAndIdempotent(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("slow", func(ctx context.Context, _ Task) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	client, cleanup := newBufconnExecutor(t, server, reporter)
	defer cleanup()

	request := &executorv1.DispatchRequest{RunId: "run-1", JobId: "job-1", Handler: "slow", CallbackToken: "token", TimeoutSeconds: 10}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	first, err := client.Dispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Dispatch waited for execution: %s", elapsed)
	}
	second, err := client.Dispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetExecutionId() == "" || second.GetExecutionId() != first.GetExecutionId() {
		t.Fatalf("duplicate dispatch was not idempotent: first=%q second=%q", first.GetExecutionId(), second.GetExecutionId())
	}
	state, err := client.Inspect(ctx, &executorv1.InspectRequest{RunId: request.GetRunId()})
	if err != nil || state.GetState() != "running" {
		t.Fatalf("Inspect() state=%q err=%v", state.GetState(), err)
	}
	close(release)
	select {
	case succeeded := <-reporter.completed:
		if !succeeded {
			t.Fatal("successful handler reported failure")
		}
	case <-time.After(time.Second):
		t.Fatal("completion was not reported")
	}
}

func TestGRPCCancelStopsExecution(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("wait", func(ctx context.Context, _ Task) error {
		defer close(stopped)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	client, cleanup := newBufconnExecutor(t, server, reporter)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := &executorv1.DispatchRequest{RunId: "run-cancel", JobId: "job-1", Handler: "wait", CallbackToken: "token", TimeoutSeconds: 10}
	if _, err = client.Dispatch(ctx, request); err != nil {
		t.Fatal(err)
	}
	response, err := client.Cancel(ctx, &executorv1.CancelRequest{RunId: request.GetRunId(), Reason: "test"})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("Cancel() accepted=%v err=%v", response.GetAccepted(), err)
	}
	state, err := client.Inspect(ctx, &executorv1.InspectRequest{RunId: request.GetRunId()})
	if err != nil || state.GetState() != "cancelled" {
		t.Fatalf("Inspect() state=%q err=%v", state.GetState(), err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cancelled handler did not stop")
	}
	select {
	case <-reporter.completed:
		t.Fatal("cancelled execution reported a conflicting completion")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestGRPCExecutionHistoryIsBounded(t *testing.T) {
	t.Parallel()
	now := time.Now()
	service := &GRPCServer{
		runs: map[string]*execution{
			"expired": {state: "succeeded", finishedAt: now.Add(-2 * time.Hour)},
			"older":   {state: "failed", finishedAt: now.Add(-2 * time.Minute)},
			"newer":   {state: "succeeded", finishedAt: now.Add(-time.Minute)},
			"active":  {state: "running"},
		},
		completed: []executionCompletion{
			{runID: "expired", finishedAt: now.Add(-2 * time.Hour)},
			{runID: "older", finishedAt: now.Add(-2 * time.Minute)},
			{runID: "newer", finishedAt: now.Add(-time.Minute)},
		},
		historyLimit:     2,
		historyRetention: time.Hour,
	}
	service.pruneExecutionHistoryLocked(now)
	if _, exists := service.runs["expired"]; exists {
		t.Fatal("expired execution was retained")
	}
	if len(service.completed) != 2 || service.runs["active"].state != "running" {
		t.Fatalf("unexpected retained history: completed=%+v runs=%+v", service.completed, service.runs)
	}
	service.historyLimit = 1
	service.pruneExecutionHistoryLocked(now)
	if _, exists := service.runs["older"]; exists || len(service.completed) != 1 {
		t.Fatalf("history limit was not enforced: completed=%+v runs=%+v", service.completed, service.runs)
	}
	service.pruneExecutionHistoryLocked(now.Add(2 * time.Hour))
	if _, exists := service.runs["newer"]; exists || len(service.completed) != 0 {
		t.Fatalf("history retention was not enforced: completed=%+v runs=%+v", service.completed, service.runs)
	}
	if _, exists := service.runs["active"]; !exists {
		t.Fatal("active execution was pruned")
	}
}

func newBufconnExecutor(t *testing.T, executor *Server, reporter Reporter) (executorv1.ExecutorServiceClient, func()) {
	t.Helper()
	service, err := NewGRPCServer(executor, reporter)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	return executorv1.NewExecutorServiceClient(connection), func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	}
}
