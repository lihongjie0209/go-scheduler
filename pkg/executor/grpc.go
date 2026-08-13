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
)

type GRPCServerOptions struct {
	MaxConcurrentExecutions int
}

// GRPCServer is the executor-facing control plane. Dispatch acknowledges after
// the task has been durably accepted into this process; completion is reported
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
	return &GRPCServer{
		server: server, reporter: reporter, runs: make(map[string]*execution),
		historyLimit: defaultExecutionHistoryLimit, historyRetention: defaultExecutionHistoryRetention,
		completionMaxAttempts: defaultCompletionMaxAttempts, completionAttemptTimeout: defaultCompletionAttemptTimeout,
		completionReportTimeout: defaultCompletionReportTimeout, completionInitialBackoff: defaultCompletionInitialBackoff,
		completionMaxBackoff: defaultCompletionMaxBackoff,
		executionSlots:       make(chan struct{}, configuration.MaxConcurrentExecutions),
	}, nil
}

func (s *GRPCServer) Dispatch(_ context.Context, request *executorv1.DispatchRequest) (*executorv1.DispatchResponse, error) {
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
	s.mu.Lock()
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
		return
	}
	s.reportCompletion(request, handlerErr == nil, message)
}

func (s *GRPCServer) reportCompletion(request *executorv1.DispatchRequest, succeeded bool, message string) {
	reportCtx, cancelReport := context.WithTimeout(context.Background(), s.completionReportTimeout)
	defer cancelReport()
	backoff := s.completionInitialBackoff
	for attempt := 1; attempt <= s.completionMaxAttempts; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(reportCtx, s.completionAttemptTimeout)
		err := s.reporter.Complete(attemptCtx, request.GetRunId(), request.GetCallbackToken(), succeeded, message)
		cancelAttempt()
		if err == nil || isPermanentCompletionError(err) {
			return
		}
		if attempt == s.completionMaxAttempts {
			slog.Error("executor completion report exhausted", "run_id", request.GetRunId(), "attempts", attempt, "error", err)
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-reportCtx.Done():
			timer.Stop()
			slog.Error("executor completion report timed out", "run_id", request.GetRunId(), "attempts", attempt, "error", reportCtx.Err())
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, s.completionMaxBackoff)
	}
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
	return &executorv1.CancelResponse{Accepted: true}, nil
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
