package executor

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type recordingReporter struct {
	mu        sync.Mutex
	completed chan bool
}

type scriptedReporter struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (r *scriptedReporter) AppendLog(context.Context, string, string, string, string) error {
	return nil
}
func (r *scriptedReporter) Complete(context.Context, string, string, bool, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := r.calls
	r.calls++
	if index < len(r.results) {
		return r.results[index]
	}
	return nil
}
func (r *scriptedReporter) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type blockingReporter struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	once    sync.Once
}

type blockingCompletionStore struct {
	started chan struct{}
	release chan struct{}
}

type selectiveCompletionReporter struct {
	delivered chan string
}

type failingCompletionStore struct{}

func (*failingCompletionStore) Save(context.Context, CompletionRecord) error {
	return errors.New("disk full")
}
func (*failingCompletionStore) List(context.Context) ([]CompletionRecord, error) { return nil, nil }
func (*failingCompletionStore) Delete(context.Context, string) error             { return nil }

func (*selectiveCompletionReporter) AppendLog(context.Context, string, string, string, string) error {
	return nil
}
func (r *selectiveCompletionReporter) Complete(ctx context.Context, runID, _ string, _ bool, _ string) error {
	if runID == "blocked-run" {
		<-ctx.Done()
		return ctx.Err()
	}
	r.delivered <- runID
	return nil
}

func (s *blockingCompletionStore) Save(ctx context.Context, _ CompletionRecord) error {
	close(s.started)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*blockingCompletionStore) List(context.Context) ([]CompletionRecord, error) { return nil, nil }
func (*blockingCompletionStore) Delete(context.Context, string) error             { return nil }

func (r *blockingReporter) AppendLog(context.Context, string, string, string, string) error {
	return nil
}
func (r *blockingReporter) Complete(ctx context.Context, _ string, _ string, _ bool, _ string) error {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	r.once.Do(func() {
		if r.started != nil {
			close(r.started)
		}
	})
	<-ctx.Done()
	return ctx.Err()
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

func TestGRPCDispatchCarriesDockerRegistryCredentials(t *testing.T) {
	t.Parallel()
	tasks := make(chan Task, 1)
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("capture", func(_ context.Context, task Task) error {
		tasks <- task
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	client, cleanup := newBufconnExecutor(t, server, reporter)
	defer cleanup()

	request := &executorv1.DispatchRequest{RunId: "registry-run", JobId: "job-1", Handler: "capture", CallbackToken: "token", TimeoutSeconds: 10, DockerRegistryAuth: &executorv1.DockerRegistryAuth{Server: "registry.example.com", Username: "robot", Password: "secret"}}
	if _, err = client.Dispatch(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case task := <-tasks:
		if task.DockerRegistryAuth == nil || task.DockerRegistryAuth.Server != "registry.example.com" || task.DockerRegistryAuth.Username != "robot" || task.DockerRegistryAuth.Password != "secret" {
			t.Fatalf("Docker registry credentials = %+v", task.DockerRegistryAuth)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive dispatched task")
	}
}

func TestGRPCDispatchEnforcesConcurrencyAndBusyState(t *testing.T) {
	t.Parallel()
	started := make(chan string, 2)
	release := make(chan struct{}, 2)
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("bounded", func(ctx context.Context, task Task) error {
		started <- task.RunID
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 2)}
	service, err := NewGRPCServer(server, reporter, GRPCServerOptions{MaxConcurrentExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := &executorv1.DispatchRequest{RunId: "bounded-1", JobId: "job-1", Handler: "bounded", CallbackToken: "token-1", TimeoutSeconds: 10}
	second := &executorv1.DispatchRequest{RunId: "bounded-2", JobId: "job-1", Handler: "bounded", CallbackToken: "token-2", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first execution did not start")
	}
	duplicate, err := service.Dispatch(t.Context(), first)
	if err != nil || duplicate.GetState() != "running" {
		t.Fatalf("duplicate dispatch = %+v, %v", duplicate, err)
	}
	if _, err = service.Dispatch(t.Context(), second); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second dispatch error = %v, want ResourceExhausted", err)
	}
	idle := httptest.NewRecorder()
	server.idle(idle, httptest.NewRequest(http.MethodGet, "/idle?job_id=job-1", nil))
	if idle.Code != http.StatusConflict {
		t.Fatalf("busy probe status = %d, want %d", idle.Code, http.StatusConflict)
	}
	release <- struct{}{}
	select {
	case <-reporter.completed:
	case <-time.After(time.Second):
		t.Fatal("first completion was not reported")
	}
	if _, err = service.Dispatch(t.Context(), second); err != nil {
		t.Fatalf("dispatch after slot release: %v", err)
	}
	release <- struct{}{}
}

func TestNewGRPCServerRejectsInvalidConcurrency(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	if _, err = NewGRPCServer(server, reporter, GRPCServerOptions{}); err == nil {
		t.Fatal("zero executor concurrency was accepted")
	}
}

func TestGRPCExecutionSlotReleasesBeforeCompletionReport(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("instant", func(context.Context, Task) error { return nil }); err != nil {
		t.Fatal(err)
	}
	reporter := &blockingReporter{started: make(chan struct{})}
	service, err := NewGRPCServer(server, reporter, GRPCServerOptions{MaxConcurrentExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	service.completionMaxAttempts = 1
	service.completionAttemptTimeout = 100 * time.Millisecond
	service.completionReportTimeout = 100 * time.Millisecond
	first := &executorv1.DispatchRequest{RunId: "report-1", JobId: "job-1", Handler: "instant", CallbackToken: "token-1", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reporter.started:
	case <-time.After(time.Second):
		t.Fatal("completion report did not start")
	}
	second := &executorv1.DispatchRequest{RunId: "report-2", JobId: "job-1", Handler: "instant", CallbackToken: "token-2", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), second); err != nil {
		t.Fatalf("execution slot remained occupied during completion report: %v", err)
	}
}

func TestGRPCCompletionIsPersistedBeforeTerminalState(t *testing.T) {
	t.Parallel()
	store := &blockingCompletionStore{started: make(chan struct{}), release: make(chan struct{})}
	service := completionRetryTestServer(&recordingReporter{completed: make(chan bool, 1)})
	service.completionStore = store
	service.completionWake = make(chan struct{}, 1)
	service.historyLimit = defaultExecutionHistoryLimit
	service.historyRetention = defaultExecutionHistoryRetention
	service.runs = map[string]*execution{"durable-order": {cancel: func() {}, executionID: "execution", state: "running"}}
	request := &executorv1.DispatchRequest{RunId: "durable-order", CallbackToken: "token"}
	finished := make(chan struct{})
	go func() {
		service.finish(request, nil)
		close(finished)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("completion persistence did not start")
	}
	state, err := service.Inspect(t.Context(), &executorv1.InspectRequest{RunId: request.GetRunId()})
	if err != nil || state.GetState() != "running" {
		t.Fatalf("state before durable save = %q, %v", state.GetState(), err)
	}
	close(store.release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("completion did not finish after durable save")
	}
	state, err = service.Inspect(t.Context(), &executorv1.InspectRequest{RunId: request.GetRunId()})
	if err != nil || state.GetState() != "succeeded" {
		t.Fatalf("state after durable save = %q, %v", state.GetState(), err)
	}
}

func TestGRPCRejectsDispatchAfterCompletionPersistenceFailure(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("instant", func(context.Context, Task) error { return nil }); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	service, err := NewGRPCServer(server, reporter, GRPCServerOptions{MaxConcurrentExecutions: 1, CompletionStore: &failingCompletionStore{}})
	if err != nil {
		t.Fatal(err)
	}
	first := &executorv1.DispatchRequest{RunId: "persistence-failure-1", JobId: "job", Handler: "instant", CallbackToken: "token", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reporter.completed:
	case <-time.After(time.Second):
		t.Fatal("best-effort completion was not reported after persistence failure")
	}
	second := &executorv1.DispatchRequest{RunId: "persistence-failure-2", JobId: "job", Handler: "instant", CallbackToken: "token", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), second); status.Code(err) != codes.Unavailable {
		t.Fatalf("dispatch after persistence failure error = %v, want Unavailable", err)
	}
}

func TestGRPCDrainWaitsAndRejectsNewDispatches(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("drain", func(ctx context.Context, _ Task) error {
		close(started)
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
	service, err := NewGRPCServer(server, reporter, GRPCServerOptions{MaxConcurrentExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := &executorv1.DispatchRequest{RunId: "drain-1", JobId: "job-1", Handler: "drain", CallbackToken: "token", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	<-started
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), time.Second)
	defer cancelDrain()
	drained := make(chan error, 1)
	go func() { drained <- service.Drain(drainCtx) }()
	deadline := time.Now().Add(time.Second)
	for {
		_, err = service.Dispatch(t.Context(), &executorv1.DispatchRequest{RunId: "drain-2", JobId: "job-1", Handler: "drain", CallbackToken: "token", TimeoutSeconds: 10})
		if status.Code(err) == codes.Unavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatch was not rejected while draining: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err = <-drained:
		t.Fatalf("Drain returned before active execution finished: %v", err)
	default:
	}
	close(release)
	if err = <-drained; err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
}

func TestGRPCDrainTimeoutCancelsActiveExecutions(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	stopped := make(chan struct{})
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("cancel-on-drain", func(ctx context.Context, _ Task) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	service, err := NewGRPCServer(server, reporter, GRPCServerOptions{MaxConcurrentExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := &executorv1.DispatchRequest{RunId: "drain-timeout", JobId: "job-1", Handler: "cancel-on-drain", CallbackToken: "token", TimeoutSeconds: 10}
	if _, err = service.Dispatch(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	<-started
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelDrain()
	if err = service.Drain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain() error = %v, want deadline exceeded", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("drain timeout did not cancel active handler")
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
	case <-time.After(250 * time.Millisecond):
	}
}

func TestGRPCCancelRecoversExternalExecutionAfterRestart(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan ExternalCancellation, 1)
	if err = server.HandleExternalCancellation("kubernetes", func(_ context.Context, cancellation ExternalCancellation) error {
		received <- cancellation
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	client, cleanup := newBufconnExecutor(t, server, &recordingReporter{completed: make(chan bool, 1)})
	defer cleanup()
	request := &executorv1.CancelRequest{RunId: "restarted-run", Reason: "operator", ExternalExecutionId: "stable-execution", JobId: "job-1", ScriptLanguage: "kubernetes", KubernetesCluster: &executorv1.KubernetesCluster{AuthMode: "service_account", ApiServer: "https://k8s.example", Namespace: "work", Token: "secret"}}
	response, err := client.Cancel(t.Context(), request)
	if err != nil || !response.GetAccepted() {
		t.Fatalf("Cancel() accepted=%v err=%v", response.GetAccepted(), err)
	}
	select {
	case cancellation := <-received:
		if cancellation.RunID != request.GetRunId() || cancellation.ExternalExecutionID != request.GetExternalExecutionId() || cancellation.JobID != request.GetJobId() || cancellation.KubernetesCluster == nil || cancellation.KubernetesCluster.Token != "secret" {
			t.Fatalf("external cancellation = %+v", cancellation)
		}
	case <-time.After(time.Second):
		t.Fatal("external canceller was not invoked")
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

func TestGRPCCompletionReportRetriesTransientErrors(t *testing.T) {
	t.Parallel()
	reporter := &scriptedReporter{results: []error{
		status.Error(codes.Unavailable, "core unavailable"),
		status.Error(codes.ResourceExhausted, "core overloaded"),
		nil,
	}}
	service := completionRetryTestServer(reporter)
	service.reportCompletion(t.Context(), CompletionRecord{RunID: "retry-run", Token: "token", Succeeded: true})
	if calls := reporter.callCount(); calls != 3 {
		t.Fatalf("completion report calls = %d, want 3", calls)
	}
}

func TestGRPCCompletionReportStopsOnPermanentError(t *testing.T) {
	t.Parallel()
	for _, code := range []codes.Code{codes.PermissionDenied, codes.NotFound} {
		code := code
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			reporter := &scriptedReporter{results: []error{status.Error(code, "permanent")}}
			service := completionRetryTestServer(reporter)
			service.reportCompletion(t.Context(), CompletionRecord{RunID: "permanent-run", Token: "token", Message: "failed"})
			if calls := reporter.callCount(); calls != 1 {
				t.Fatalf("completion report calls = %d, want 1", calls)
			}
		})
	}
}

func TestGRPCCompletionReportHasAttemptLimit(t *testing.T) {
	t.Parallel()
	reporter := &scriptedReporter{results: []error{
		status.Error(codes.Unavailable, "one"), status.Error(codes.Unavailable, "two"),
		status.Error(codes.Unavailable, "three"), status.Error(codes.Unavailable, "four"),
		status.Error(codes.Unavailable, "five"), status.Error(codes.Unavailable, "six"),
	}}
	service := completionRetryTestServer(reporter)
	service.reportCompletion(t.Context(), CompletionRecord{RunID: "exhausted-run", Token: "token", Succeeded: true})
	if calls := reporter.callCount(); calls != service.completionMaxAttempts {
		t.Fatalf("completion report calls = %d, want %d", calls, service.completionMaxAttempts)
	}
}

func TestGRPCCompletionReportHasOverallDeadline(t *testing.T) {
	t.Parallel()
	reporter := &blockingReporter{}
	service := completionRetryTestServer(reporter)
	service.completionAttemptTimeout = time.Second
	service.completionReportTimeout = 20 * time.Millisecond
	started := time.Now()
	service.reportCompletion(t.Context(), CompletionRecord{RunID: "deadline-run", Token: "token", Succeeded: true})
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("completion report exceeded overall deadline: %s", elapsed)
	}
}

func TestGRPCCompletionDeliveryRecoversPersistedRecordAfterRestart(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	firstStore, err := NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	record := CompletionRecord{RunID: "restart-run", Token: "restart-token", Succeeded: true, CreatedAt: time.Now().UTC()}
	if err = firstStore.Save(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	reporter := &recordingReporter{completed: make(chan bool, 1)}
	service := completionRetryTestServer(reporter)
	service.completionStore = secondStore
	service.completionWake = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	service.RunCompletionDelivery(ctx)
	select {
	case succeeded := <-reporter.completed:
		if !succeeded {
			t.Fatal("persisted successful completion was changed")
		}
	case <-time.After(time.Second):
		t.Fatal("persisted completion was not delivered after restart")
	}
	deadline := time.Now().Add(time.Second)
	for {
		records, listErr := secondStore.List(t.Context())
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(records) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered completion was not deleted: %+v", records)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	service.WaitCompletionDelivery()
}

func TestGRPCDurableCompletionDoesNotHeadOfLineBlock(t *testing.T) {
	t.Parallel()
	store, err := NewFileCompletionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC()
	for _, record := range []CompletionRecord{
		{RunID: "blocked-run", Token: "blocked-token", CreatedAt: createdAt},
		{RunID: "healthy-run", Token: "healthy-token", Succeeded: true, CreatedAt: createdAt.Add(time.Nanosecond)},
	} {
		if err = store.Save(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	reporter := &selectiveCompletionReporter{delivered: make(chan string, 1)}
	service := completionRetryTestServer(reporter)
	service.completionStore = store
	service.completionWake = make(chan struct{}, 1)
	service.completionAttemptTimeout = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	service.RunCompletionDelivery(ctx)
	select {
	case runID := <-reporter.delivered:
		if runID != "healthy-run" {
			t.Fatalf("delivered run = %q, want healthy-run", runID)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("healthy completion was blocked behind unavailable completion")
	}
	deadline := time.Now().Add(time.Second)
	for {
		records, listErr := store.List(t.Context())
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(records) == 1 && records[0].RunID == "blocked-run" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completion outbox records = %+v", records)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	service.WaitCompletionDelivery()
}

func completionRetryTestServer(reporter Reporter) *GRPCServer {
	return &GRPCServer{
		reporter: reporter, completionMaxAttempts: 5,
		completionAttemptTimeout: 50 * time.Millisecond, completionReportTimeout: time.Second,
		completionInitialBackoff: time.Millisecond, completionMaxBackoff: 2 * time.Millisecond,
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
