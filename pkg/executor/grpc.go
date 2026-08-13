package executor

import (
	"context"
	"errors"
	"fmt"
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
)

// GRPCServer is the executor-facing control plane. Dispatch acknowledges after
// the task has been durably accepted into this process; completion is reported
// asynchronously so long-running work never occupies a Core dispatch worker.
type GRPCServer struct {
	executorv1.UnimplementedExecutorServiceServer
	server           *Server
	reporter         Reporter
	mu               sync.RWMutex
	runs             map[string]*execution
	completed        []executionCompletion
	historyLimit     int
	historyRetention time.Duration
}

func NewGRPCServer(server *Server, reporter Reporter) (*GRPCServer, error) {
	if server == nil || reporter == nil {
		return nil, errors.New("executor server and reporter are required")
	}
	return &GRPCServer{server: server, reporter: reporter, runs: make(map[string]*execution), historyLimit: defaultExecutionHistoryLimit, historyRetention: defaultExecutionHistoryRetention}, nil
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
	s.pruneExecutionHistoryLocked(time.Now())
	if current, ok := s.runs[request.GetRunId()]; ok {
		response := &executorv1.DispatchResponse{Accepted: true, ExecutionId: current.executionID, State: current.state}
		s.mu.Unlock()
		return response, nil
	}
	executionID := request.GetExternalExecutionId()
	if executionID == "" {
		executionID = uuid.NewString()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.GetTimeoutSeconds())*time.Second)
	s.runs[request.GetRunId()] = &execution{cancel: cancel, executionID: executionID, state: "running"}
	s.mu.Unlock()

	go s.execute(ctx, request, registered.handler)
	return &executorv1.DispatchResponse{Accepted: true, ExecutionId: executionID, State: "running"}, nil
}

func (s *GRPCServer) execute(ctx context.Context, request *executorv1.DispatchRequest, handler Handler) {
	defer func() {
		if recovered := recover(); recovered != nil {
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
	logger := &grpcLogger{reporter: s.reporter, runID: request.GetRunId(), token: request.GetCallbackToken()}
	err := invokeHandler(ctx, handler, Task{RunID: request.GetRunId(), ExternalExecutionID: request.GetExternalExecutionId(), JobID: request.GetJobId(), Input: request.GetInput(), BroadcastGroupID: request.GetBroadcastGroupId(), BroadcastIndex: request.GetBroadcastIndex(), BroadcastTotal: request.GetBroadcastTotal(), ScriptLanguage: request.GetScriptLanguage(), ScriptSource: request.GetScriptSource(), KubernetesCluster: kubernetes, HTTP: httpExecution, Logger: logger})
	s.finish(request, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.reporter.Complete(ctx, request.GetRunId(), request.GetCallbackToken(), handlerErr == nil, message)
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

func (s *GRPCServer) Cancel(_ context.Context, request *executorv1.CancelRequest) (*executorv1.CancelResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.runs[request.GetRunId()]
	if !exists || current.state != "running" {
		return &executorv1.CancelResponse{Accepted: false}, nil
	}
	finishedAt := time.Now()
	current.state, current.message, current.finishedAt = "cancelled", request.GetReason(), finishedAt
	current.cancel()
	s.completed = append(s.completed, executionCompletion{runID: request.GetRunId(), finishedAt: finishedAt})
	s.pruneExecutionHistoryLocked(finishedAt)
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
