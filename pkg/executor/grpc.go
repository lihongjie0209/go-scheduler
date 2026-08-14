package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reporter sends execution output back to Core. Implementations must be safe
// for concurrent use and should apply an RPC deadline to every operation.
type Reporter interface {
	AppendLog(context.Context, string, string, string, string) error
	Complete(context.Context, string, string, bool, string) error
}

type execution struct {
	cancel      context.CancelFunc
	executionID string
	state       string
	message     string
	finishedAt  time.Time
}

type executionCompletion struct {
	runID      string
	finishedAt time.Time
}

const (
	defaultExecutionHistoryLimit     = 10_000
	defaultExecutionHistoryRetention = 24 * time.Hour
	defaultCompletionMaxAttempts     = 6
	defaultCompletionAttemptTimeout  = 10 * time.Second
	defaultCompletionReportTimeout   = 2 * time.Minute
	defaultCompletionInitialBackoff  = 200 * time.Millisecond
	defaultCompletionMaxBackoff      = 5 * time.Second
	defaultMaxConcurrentExecutions   = 32
	completionDeliveryConcurrency    = 8
)

type GRPCServerOptions struct {
	MaxConcurrentExecutions int
	CompletionStore         CompletionStore
}

// GRPCServer is the executor-facing control plane. Dispatch acknowledges after
// the task has been accepted into this process; completion is reported
// asynchronously so long-running work never occupies a Core dispatch worker.
type GRPCServer struct {
	executorv1.UnimplementedExecutorServiceServer
	server                   *Server
	reporter                 Reporter
	mu                       sync.RWMutex
	runs                     map[string]*execution
	draining                 bool
	executionWG              sync.WaitGroup
	completed                []executionCompletion
	historyLimit             int
	historyRetention         time.Duration
	completionMaxAttempts    int
	completionAttemptTimeout time.Duration
	completionReportTimeout  time.Duration
	completionInitialBackoff time.Duration
	completionMaxBackoff     time.Duration
	executionSlots           chan struct{}
	completionStore          CompletionStore
	executionStore           ExecutionStore
	completionWake           chan struct{}
	completionDeliveryWG     sync.WaitGroup
	completionDeliveryOnce   sync.Once
	completionStoreFailed    bool
}

func NewGRPCServer(server *Server, reporter Reporter, options ...GRPCServerOptions) (*GRPCServer, error) {
	if server == nil || reporter == nil {
		return nil, errors.New("executor server and reporter are required")
	}
	configuration := GRPCServerOptions{MaxConcurrentExecutions: defaultMaxConcurrentExecutions}
	if len(options) > 1 {
		return nil, errors.New("at most one gRPC server options value is supported")
	}
	if len(options) == 1 {
		configuration = options[0]
	}
	if configuration.MaxConcurrentExecutions < 1 {
		return nil, errors.New("maximum concurrent executions must be positive")
	}
	result := &GRPCServer{
		server: server, reporter: reporter, runs: make(map[string]*execution),
		historyLimit: defaultExecutionHistoryLimit, historyRetention: defaultExecutionHistoryRetention,
		completionMaxAttempts: defaultCompletionMaxAttempts, completionAttemptTimeout: defaultCompletionAttemptTimeout,
		completionReportTimeout: defaultCompletionReportTimeout, completionInitialBackoff: defaultCompletionInitialBackoff,
		completionMaxBackoff: defaultCompletionMaxBackoff,
		executionSlots:       make(chan struct{}, configuration.MaxConcurrentExecutions),
		completionStore:      configuration.CompletionStore,
		completionWake:       make(chan struct{}, 1),
	}
	result.executionStore, _ = configuration.CompletionStore.(ExecutionStore)
	return result, nil
}

func (s *GRPCServer) Dispatch(ctx context.Context, request *executorv1.DispatchRequest) (*executorv1.DispatchResponse, error) {
	if request.GetRunId() == "" || request.GetJobId() == "" || request.GetHandler() == "" || request.GetCallbackToken() == "" || request.GetTimeoutSeconds() < 1 || request.GetTimeoutSeconds() > 86400 {
		return nil, status.Error(codes.InvalidArgument, "invalid dispatch request")
	}
	s.server.mu.RLock()
	registered, exists := s.server.handlers[request.GetHandler()]
	s.server.mu.RUnlock()
	if !exists {
		return nil, status.Errorf(codes.NotFound, "handler %q not found", request.GetHandler())
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "executor is draining")
	}
	if s.completionStoreFailed {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "executor completion persistence is unavailable")
	}
	s.pruneExecutionHistoryLocked(time.Now())
	if current, ok := s.runs[request.GetRunId()]; ok {
		response := &executorv1.DispatchResponse{Accepted: true, ExecutionId: current.executionID, State: current.state}
		s.mu.Unlock()
		return response, nil
	}
	select {
	case s.executionSlots <- struct{}{}:
	default:
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "executor concurrency limit reached")
	}
	if s.executionStore != nil {
		if err := s.executionStore.SaveExecution(ctx, request); err != nil {
			<-s.executionSlots
			s.completionStoreFailed = true
			s.mu.Unlock()
			return nil, status.Errorf(codes.Unavailable, "persist accepted execution: %v", err)
		}
	}
	executionID := request.GetExternalExecutionId()
	if executionID == "" {
		executionID = uuid.NewString()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.GetTimeoutSeconds())*time.Second)
	s.runs[request.GetRunId()] = &execution{cancel: cancel, executionID: executionID, state: "running"}
	s.executionWG.Add(1)
	s.mu.Unlock()

	go s.execute(ctx, request, registered.handler)
	return &executorv1.DispatchResponse{Accepted: true, ExecutionId: executionID, State: "running"}, nil
}

func (s *GRPCServer) execute(ctx context.Context, request *executorv1.DispatchRequest, handler Handler) {
	defer s.executionWG.Done()
	s.server.markActive(request.GetJobId(), 1)
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		s.server.markActive(request.GetJobId(), -1)
		<-s.executionSlots
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			release()
			s.finish(request, fmt.Errorf("handler panic: %v", recovered))
		}
	}()
	cluster := request.GetKubernetesCluster()
	var kubernetes *KubernetesClusterConfig
	if cluster != nil {
		kubernetes = &KubernetesClusterConfig{AuthMode: cluster.GetAuthMode(), APIServer: cluster.GetApiServer(), Namespace: cluster.GetNamespace(), Kubeconfig: cluster.GetKubeconfig(), Token: cluster.GetToken(), CAData: cluster.GetCaData(), InsecureSkipTLSVerify: cluster.GetInsecureSkipTlsVerify()}
	}
	httpSpec := request.GetHttp()
	var httpExecution *HTTPExecution
	if httpSpec != nil {
		httpExecution = &HTTPExecution{URL: httpSpec.GetUrl(), Method: httpSpec.GetMethod(), Headers: httpSpec.GetHeaders(), Body: httpSpec.GetBody()}
	}
	var dockerRegistryAuth *DockerRegistryAuth
	if auth := request.GetDockerRegistryAuth(); auth != nil {
		dockerRegistryAuth = &DockerRegistryAuth{Server: auth.GetServer(), Username: auth.GetUsername(), Password: auth.GetPassword()}
	}
	logger := &grpcLogger{reporter: s.reporter, runID: request.GetRunId(), token: request.GetCallbackToken()}
	err := invokeHandler(ctx, handler, Task{RunID: request.GetRunId(), ExternalExecutionID: request.GetExternalExecutionId(), JobID: request.GetJobId(), Input: request.GetInput(), BroadcastGroupID: request.GetBroadcastGroupId(), BroadcastIndex: request.GetBroadcastIndex(), BroadcastTotal: request.GetBroadcastTotal(), ScriptLanguage: request.GetScriptLanguage(), ScriptSource: request.GetScriptSource(), KubernetesCluster: kubernetes, HTTP: httpExecution, DockerRegistryAuth: dockerRegistryAuth, Logger: logger})
	release()
	s.finish(request, err)
}

// Drain rejects new dispatches and waits for accepted executions to finish.
// If ctx expires, all still-running handlers are cancelled before returning.
func (s *GRPCServer) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.executionWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for _, current := range s.runs {
			if current.state == "running" {
				current.cancel()
			}
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *GRPCServer) finish(request *executorv1.DispatchRequest, handlerErr error) {
	state, message := "succeeded", ""
	if handlerErr != nil {
		state, message = "failed", truncate(handlerErr.Error(), 4096)
	}
	record := CompletionRecord{RunID: request.GetRunId(), Token: request.GetCallbackToken(), Succeeded: handlerErr == nil, Message: message, CreatedAt: time.Now().UTC()}
	persisted := false
	var persistErr error
	if s.completionStore != nil {
		s.mu.RLock()
		current := s.runs[request.GetRunId()]
		shouldPersist := current != nil && current.state == "running"
		s.mu.RUnlock()
		if !shouldPersist {
			return
		}
		persistErr = s.completionStore.Save(context.Background(), record)
		persisted = persistErr == nil
		if persisted && s.executionStore != nil {
			if cleanupErr := s.executionStore.DeleteExecution(context.Background(), request.GetRunId()); cleanupErr != nil {
				slog.Error("delete completed executor execution", "run_id", request.GetRunId(), "error", cleanupErr)
				s.mu.Lock()
				s.completionStoreFailed = true
				s.mu.Unlock()
			}
		}
		if persistErr != nil {
			slog.Error("persist executor completion", "run_id", request.GetRunId(), "error", persistErr)
		}
	}
	s.mu.Lock()
	if persistErr != nil {
		s.completionStoreFailed = true
	}
	current, exists := s.runs[request.GetRunId()]
	transitioned := false
	if exists && current.state == "running" {
		finishedAt := time.Now()
		current.state, current.message, current.finishedAt = state, message, finishedAt
		current.cancel()
		s.completed = append(s.completed, executionCompletion{runID: request.GetRunId(), finishedAt: finishedAt})
		s.pruneExecutionHistoryLocked(finishedAt)
		transitioned = true
	}
	s.mu.Unlock()
	if !transitioned {
		if persisted {
			s.wakeCompletionDelivery()
		}
		return
	}
	if s.completionStore == nil {
		s.reportCompletion(context.Background(), record)
		return
	}
	if persistErr != nil {
		// Retain the previous in-memory retry as a best effort when local
		// persistence itself is unavailable.
		s.reportCompletion(context.Background(), record)
		return
	}
	s.wakeCompletionDelivery()
}

func (s *GRPCServer) reportCompletion(ctx context.Context, record CompletionRecord) bool {
	reportCtx, cancelReport := context.WithTimeout(ctx, s.completionReportTimeout)
	defer cancelReport()
	backoff := s.completionInitialBackoff
	for attempt := 1; attempt <= s.completionMaxAttempts; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(reportCtx, s.completionAttemptTimeout)
		err := s.reporter.Complete(attemptCtx, record.RunID, record.Token, record.Succeeded, record.Message)
		cancelAttempt()
		if err == nil || isPermanentCompletionError(err) {
			return true
		}
		if attempt == s.completionMaxAttempts {
			slog.Error("executor completion report exhausted", "run_id", record.RunID, "attempts", attempt, "error", err)
			return false
		}
		timer := time.NewTimer(backoff)
		select {
		case <-reportCtx.Done():
			timer.Stop()
			slog.Error("executor completion report timed out", "run_id", record.RunID, "attempts", attempt, "error", reportCtx.Err())
			return false
		case <-timer.C:
		}
		backoff = min(backoff*2, s.completionMaxBackoff)
	}
	return false
}

func (s *GRPCServer) RunCompletionDelivery(ctx context.Context) {
	if s.completionStore == nil {
		return
	}
	s.completionDeliveryOnce.Do(func() {
		s.completionDeliveryWG.Add(1)
		go func() {
			defer s.completionDeliveryWG.Done()
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.completionWake:
				case <-timer.C:
				}
				s.deliverPersistedCompletions(ctx)
				timer.Reset(time.Second)
			}
		}()
	})
}

func (s *GRPCServer) WaitCompletionDelivery() { s.completionDeliveryWG.Wait() }

// RecoverExecutions reconciles work accepted before a process restart. Work
// managed by Docker or Kubernetes is safe to reattach to; process-local work
// is failed explicitly so Core can apply its configured retry policy.
func (s *GRPCServer) RecoverExecutions(ctx context.Context) error {
	if s.executionStore == nil {
		return nil
	}
	completions, err := s.completionStore.List(ctx)
	if err != nil {
		return fmt.Errorf("list completions before execution recovery: %w", err)
	}
	completed := make(map[string]struct{}, len(completions))
	for _, record := range completions {
		completed[record.RunID] = struct{}{}
	}
	requests, err := s.executionStore.ListExecutions(ctx)
	if err != nil {
		return fmt.Errorf("list executions for recovery: %w", err)
	}
	for _, request := range requests {
		if _, ok := completed[request.GetRunId()]; ok {
			if err = s.executionStore.DeleteExecution(ctx, request.GetRunId()); err != nil {
				return fmt.Errorf("remove completed execution %q: %w", request.GetRunId(), err)
			}
			continue
		}
		if isExternallyRecoverable(request) {
			continue
		}
		record := CompletionRecord{RunID: request.GetRunId(), Token: request.GetCallbackToken(), Succeeded: false, Message: "executor restarted before process-local execution completed", CreatedAt: time.Now().UTC()}
		if err = s.completionStore.Save(ctx, record); err != nil {
			return fmt.Errorf("persist interrupted execution %q: %w", request.GetRunId(), err)
		}
		if err = s.executionStore.DeleteExecution(ctx, request.GetRunId()); err != nil {
			return fmt.Errorf("remove interrupted execution %q: %w", request.GetRunId(), err)
		}
	}
	for _, request := range requests {
		if _, ok := completed[request.GetRunId()]; ok || !isExternallyRecoverable(request) {
			continue
		}
		if _, err = s.Dispatch(ctx, request); err != nil {
			return fmt.Errorf("resume external execution %q: %w", request.GetRunId(), err)
		}
	}
	return nil
}

func isExternallyRecoverable(request *executorv1.DispatchRequest) bool {
	switch request.GetScriptLanguage() {
	case "docker", "kubernetes":
		return true
	default:
		return false
	}
}

func (s *GRPCServer) wakeCompletionDelivery() {
	select {
	case s.completionWake <- struct{}{}:
	default:
	}
}

func (s *GRPCServer) deliverPersistedCompletions(ctx context.Context) {
	records, err := s.completionStore.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("list persisted executor completions", "error", err)
		}
		return
	}
	semaphore := make(chan struct{}, completionDeliveryConcurrency)
	var group sync.WaitGroup
	for _, record := range records {
		select {
		case semaphore <- struct{}{}:
			group.Add(1)
			go func(record CompletionRecord) {
				defer group.Done()
				defer func() { <-semaphore }()
				if !s.reportCompletionOnce(ctx, record) {
					return
				}
				if deleteErr := s.completionStore.Delete(ctx, record.RunID); deleteErr != nil && ctx.Err() == nil {
					slog.Error("delete delivered executor completion", "run_id", record.RunID, "error", deleteErr)
				}
			}(record)
		case <-ctx.Done():
			group.Wait()
			return
		}
	}
	group.Wait()
}

func (s *GRPCServer) reportCompletionOnce(ctx context.Context, record CompletionRecord) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, s.completionAttemptTimeout)
	defer cancel()
	err := s.reporter.Complete(attemptCtx, record.RunID, record.Token, record.Succeeded, record.Message)
	if err == nil || isPermanentCompletionError(err) {
		return true
	}
	if ctx.Err() == nil {
		slog.Warn("executor completion remains pending", "run_id", record.RunID, "error", err)
	}
	return false
}

func isPermanentCompletionError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented:
		return true
	default:
		return false
	}
}

func (s *GRPCServer) pruneExecutionHistoryLocked(now time.Time) {
	for len(s.completed) > 0 {
		oldest := s.completed[0]
		overLimit := s.historyLimit >= 0 && len(s.completed) > s.historyLimit
		expired := s.historyRetention > 0 && now.Sub(oldest.finishedAt) >= s.historyRetention
		if !overLimit && !expired {
			break
		}
		if current, exists := s.runs[oldest.runID]; exists && current.state != "running" && current.finishedAt.Equal(oldest.finishedAt) {
			delete(s.runs, oldest.runID)
		}
		s.completed[0] = executionCompletion{}
		s.completed = s.completed[1:]
	}
	if len(s.completed) == 0 {
		s.completed = nil
	}
}

func (s *GRPCServer) Cancel(ctx context.Context, request *executorv1.CancelRequest) (*executorv1.CancelResponse, error) {
	if request.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	s.mu.Lock()
	current, exists := s.runs[request.GetRunId()]
	cancelledInMemory := false
	if exists && current.state == "running" {
		finishedAt := time.Now()
		current.state, current.message, current.finishedAt = "cancelled", request.GetReason(), finishedAt
		current.cancel()
		s.completed = append(s.completed, executionCompletion{runID: request.GetRunId(), finishedAt: finishedAt})
		s.pruneExecutionHistoryLocked(finishedAt)
		cancelledInMemory = true
	} else if exists && current.state == "cancelled" {
		cancelledInMemory = true
	}
	s.mu.Unlock()

	if request.GetExternalExecutionId() == "" || request.GetJobId() == "" || request.GetScriptLanguage() == "" {
		if cancelledInMemory {
			if err := s.deletePersistedExecution(ctx, request.GetRunId()); err != nil {
				return nil, status.Errorf(codes.Unavailable, "persist cancellation: %v", err)
			}
		}
		return &executorv1.CancelResponse{Accepted: cancelledInMemory}, nil
	}
	s.server.mu.RLock()
	canceller := s.server.cancellers[request.GetScriptLanguage()]
	s.server.mu.RUnlock()
	if canceller == nil {
		if request.GetScriptLanguage() == "docker" || request.GetScriptLanguage() == "kubernetes" {
			return &executorv1.CancelResponse{Accepted: false}, nil
		}
		// Script and HTTP work cannot survive an executor process restart. If
		// no in-memory execution exists, their desired cancelled state is
		// already satisfied.
		if err := s.deletePersistedExecution(ctx, request.GetRunId()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "persist cancellation: %v", err)
		}
		return &executorv1.CancelResponse{Accepted: true}, nil
	}
	var cluster *KubernetesClusterConfig
	if value := request.GetKubernetesCluster(); value != nil {
		cluster = &KubernetesClusterConfig{AuthMode: value.GetAuthMode(), APIServer: value.GetApiServer(), Namespace: value.GetNamespace(), Kubeconfig: value.GetKubeconfig(), Token: value.GetToken(), CAData: value.GetCaData(), InsecureSkipTLSVerify: value.GetInsecureSkipTlsVerify()}
	}
	cancellation := ExternalCancellation{RunID: request.GetRunId(), ExternalExecutionID: request.GetExternalExecutionId(), JobID: request.GetJobId(), ScriptLanguage: request.GetScriptLanguage(), KubernetesCluster: cluster}
	if err := canceller(ctx, cancellation); err != nil {
		return nil, status.Errorf(codes.Unavailable, "cancel external execution: %v", err)
	}
	if err := s.deletePersistedExecution(ctx, request.GetRunId()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "persist external cancellation: %v", err)
	}
	return &executorv1.CancelResponse{Accepted: true}, nil
}

func (s *GRPCServer) deletePersistedExecution(ctx context.Context, runID string) error {
	if s.executionStore == nil {
		return nil
	}
	if err := s.executionStore.DeleteExecution(ctx, runID); err != nil {
		s.mu.Lock()
		s.completionStoreFailed = true
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *GRPCServer) Inspect(_ context.Context, request *executorv1.InspectRequest) (*executorv1.ExecutionState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	current, exists := s.runs[request.GetRunId()]
	if !exists {
		return nil, status.Error(codes.NotFound, "execution not found")
	}
	return &executorv1.ExecutionState{RunId: request.GetRunId(), ExecutionId: current.executionID, State: current.state, Message: current.message}, nil
}

type grpcLogger struct {
	reporter     Reporter
	runID, token string
}

func (l *grpcLogger) Info(content string) error  { return l.write("stdout", content) }
func (l *grpcLogger) Error(content string) error { return l.write("stderr", content) }
func (l *grpcLogger) write(stream, content string) error {
	if len(content) > 65536 {
		return errors.New("log content exceeds 65536 bytes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return l.reporter.AppendLog(ctx, l.runID, l.token, stream, content)
}
