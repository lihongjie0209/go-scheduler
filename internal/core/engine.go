package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/credentials"
)

type Engine struct {
	store            *store.Store
	owner            string
	interval         time.Duration
	workers          int
	publicBaseURL    string
	client           *http.Client
	wg               sync.WaitGroup
	historyRetention time.Duration
	lastCleanup      time.Time
	lastRunCleanup   time.Time
	lastPartitionRun time.Time
	dispatchWake     chan struct{}
	executorGRPC     *executorGRPCPool
}

const auxiliaryHistoryCleanupInterval = 10 * time.Second

const (
	executorCommandBatchSize   = 16
	executorCommandConcurrency = 8
	executorCommandMaxPoll     = time.Second
	executorCommandTimeout     = 5 * time.Second
	cancelWatchInterval        = 2 * time.Second
	persistWriteTimeout        = 10 * time.Second
)

func persistContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), persistWriteTimeout)
}

func persistWrite(parent context.Context, write func(context.Context) error) error {
	ctx, cancel := persistContext(parent)
	defer cancel()
	return write(ctx)
}

type EngineOption func(*Engine)

func WithHTTPClient(client *http.Client) EngineOption {
	return func(engine *Engine) { engine.client = client }
}

func WithExecutorGRPC(token string) EngineOption {
	return func(engine *Engine) { engine.executorGRPC = newExecutorGRPCPool(token) }
}

func WithExecutorGRPCTransport(token string, transport credentials.TransportCredentials) EngineOption {
	return func(engine *Engine) { engine.executorGRPC = newExecutorGRPCPoolWithTransport(token, transport) }
}

func WithExecutorController(controller *ExecutorController) EngineOption {
	return func(engine *Engine) {
		if controller != nil {
			engine.executorGRPC = controller.pool
		}
	}
}

func NewEngine(s *store.Store, owner string, interval time.Duration, workers int, publicBaseURL string, historyRetention time.Duration, _ []string, options ...EngineOption) *Engine {
	engine := &Engine{store: s, owner: owner, interval: interval, workers: workers, publicBaseURL: strings.TrimRight(publicBaseURL, "/"), historyRetention: historyRetention, dispatchWake: make(chan struct{}, 1)}
	for _, option := range options {
		option(engine)
	}
	if engine.client == nil {
		transport := &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second}
		engine.client = &http.Client{Transport: transport}
	}
	return engine
}

func (e *Engine) Run(ctx context.Context) {
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.loop(ctx) }()
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.maintenanceLoop(ctx) }()
	if e.executorGRPC != nil {
		e.wg.Add(1)
		go func() { defer e.wg.Done(); e.executorCommandLoop(ctx) }()
	}
}
func (e *Engine) Wait() {
	e.wg.Wait()
	if e.executorGRPC != nil {
		e.executorGRPC.close()
	}
}
func (e *Engine) loop(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	sem := make(chan struct{}, e.workers)
	if err := e.tick(ctx, sem); err != nil && ctx.Err() == nil {
		slog.Error("scheduler tick failed", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.tick(ctx, sem); err != nil && ctx.Err() == nil {
				slog.Error("scheduler tick failed", "error", err)
			}
		case <-e.dispatchWake:
			if err := e.dispatch(ctx, sem); err != nil && ctx.Err() == nil {
				slog.Error("scheduler dispatch failed", "error", err)
			}
		}
	}
}
func (e *Engine) tick(ctx context.Context, sem chan struct{}) error {
	if err := e.store.EnqueueDue(ctx, 500); err != nil {
		return fmt.Errorf("enqueue due jobs: %w", err)
	}
	if err := e.store.ExpireCallbacks(ctx); err != nil {
		return fmt.Errorf("expire callbacks: %w", err)
	}
	available := availableWorkerSlots(sem)
	if available == 0 {
		observability.WorkerSaturationTicks.Inc()
		return nil
	}
	return e.dispatch(ctx, sem)
}

func (e *Engine) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(auxiliaryHistoryCleanupInterval)
	defer ticker.Stop()
	e.maintain(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.maintain(ctx)
		}
	}
}

func (e *Engine) maintain(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if time.Since(e.lastPartitionRun) >= time.Hour {
		result, err := e.store.MaintainRunPartitions(ctx, time.Now(), e.historyRetention)
		if err != nil {
			slog.Error("partition maintenance failed", "error", err)
		} else {
			slog.Info("partition maintenance completed", "backend", result.Backend, "dropped", result.Dropped)
			e.lastPartitionRun = time.Now()
		}
	}
	if time.Since(e.lastCleanup) >= auxiliaryHistoryCleanupInterval {
		if err := e.store.CleanupAuxiliaryHistory(ctx, e.historyRetention); err != nil {
			slog.Error("cleanup auxiliary history", "error", err)
		} else {
			e.lastCleanup = time.Now()
		}
	}
	if time.Since(e.lastRunCleanup) >= time.Hour {
		if err := e.store.CleanupRunHistory(ctx, e.historyRetention); err != nil {
			slog.Error("cleanup run history", "error", err)
		} else {
			e.lastRunCleanup = time.Now()
		}
	}
}

func (e *Engine) executorCommandLoop(ctx context.Context) {
	poll := min(e.interval, executorCommandMaxPoll)
	if poll <= 0 {
		poll = executorCommandMaxPoll
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		claimed := e.processExecutorCommands(ctx)
		if ctx.Err() != nil {
			return
		}
		if claimed == executorCommandBatchSize {
			timer.Reset(0)
		} else {
			timer.Reset(poll)
		}
	}
}

func (e *Engine) processExecutorCommands(ctx context.Context) int {
	commands, err := e.store.ClaimExecutorCommands(ctx, e.owner, executorCommandBatchSize)
	if err != nil {
		slog.Error("claim executor commands", "error", err)
		return 0
	}
	semaphore := make(chan struct{}, executorCommandConcurrency)
	var group sync.WaitGroup
	for _, command := range commands {
		if ctx.Err() != nil {
			break
		}
		semaphore <- struct{}{}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-semaphore }()
			commandCtx, cancel := context.WithTimeout(ctx, executorCommandTimeout)
			request := &executorv1.CancelRequest{RunId: command.RunID, Reason: command.Reason, ExternalExecutionId: command.ExternalExecutionID, JobId: command.JobID, ScriptLanguage: command.ScriptLanguage}
			var deliverErr error
			if command.ScriptLanguage == "kubernetes" {
				cluster, clusterErr := e.store.GetKubernetesCluster(commandCtx, command.TenantID, command.KubernetesClusterID)
				if clusterErr != nil {
					deliverErr = fmt.Errorf("load Kubernetes cancellation target: %w", clusterErr)
				} else {
					request.KubernetesCluster = &executorv1.KubernetesCluster{AuthMode: cluster.AuthMode, ApiServer: cluster.APIServer, Namespace: cluster.Namespace, Kubeconfig: cluster.Credentials.Kubeconfig, Token: cluster.Credentials.Token, CaData: cluster.Credentials.CAData, InsecureSkipTlsVerify: cluster.InsecureSkipTLSVerify}
				}
			}
			if deliverErr == nil {
				deliverErr = e.executorGRPC.cancel(commandCtx, command.ExecutorAddress, request)
			}
			cancel()
			if deliverErr == nil {
				if completeErr := persistWrite(ctx, func(ackCtx context.Context) error {
					return e.store.CompleteExecutorCommand(ackCtx, e.owner, command.ID)
				}); completeErr != nil && !errors.Is(completeErr, store.ErrConflict) {
					slog.Error("complete executor command", "command_id", command.ID, "run_id", command.RunID, "error", completeErr)
				}
				return
			}
			delay := executorCommandRetryDelay(command.Attempts)
			if retryErr := persistWrite(ctx, func(ackCtx context.Context) error {
				return e.store.RetryExecutorCommand(ackCtx, e.owner, command.ID, deliverErr.Error(), delay)
			}); retryErr != nil && !errors.Is(retryErr, store.ErrConflict) {
				slog.Error("retry executor command", "command_id", command.ID, "run_id", command.RunID, "error", retryErr)
			}
		}()
	}
	group.Wait()
	return len(commands)
}

func executorCommandRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := min(attempt-1, 9)
	return min(time.Second*time.Duration(1<<shift), 5*time.Minute)
}

func (e *Engine) dispatch(ctx context.Context, sem chan struct{}) error {
	available := availableWorkerSlots(sem)
	if available == 0 {
		return nil
	}
	claimStartedAt := time.Now()
	runs, err := e.store.ClaimRuns(ctx, e.owner, available, 2*time.Minute)
	observeRunClaim(claimStartedAt, len(runs), err)
	if err != nil {
		return fmt.Errorf("claim runs: %w", err)
	}
	for _, claimed := range runs {
		select {
		case sem <- struct{}{}:
			e.wg.Add(1)
			go func(c store.ClaimedRun) {
				defer e.wg.Done()
				defer e.releaseWorker(sem)
				e.execute(ctx, c)
			}(claimed)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func observeRunClaim(startedAt time.Time, count int, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	observability.RunClaimAttempts.WithLabelValues(outcome).Inc()
	observability.RunClaimDuration.WithLabelValues(outcome).Observe(time.Since(startedAt).Seconds())
	if count > 0 {
		observability.RunClaims.Add(float64(count))
	}
}

func (e *Engine) WakeDispatch() {
	if e == nil || e.dispatchWake == nil {
		return
	}
	select {
	case e.dispatchWake <- struct{}{}:
	default:
	}
}

func (e *Engine) releaseWorker(sem chan struct{}) {
	<-sem
	if !shouldWakeDispatcher(len(sem), cap(sem)) {
		return
	}
	select {
	case e.dispatchWake <- struct{}{}:
	default:
	}
}

func shouldWakeDispatcher(active, capacity int) bool {
	if active == 0 {
		return true
	}
	threshold := max(capacity/4, 1)
	return capacity-active >= threshold
}

func availableWorkerSlots(sem chan struct{}) int {
	return cap(sem) - len(sem)
}

func (e *Engine) routeClaimedExecutor(ctx context.Context, c store.ClaimedRun) (store.ExecutorNode, error) {
	var requiredLabels, excludedLabels []string
	var routeErr error
	strategy := ""
	var activeNodes []store.ExecutorNode
	if c.Run.BroadcastGroupID != "" {
		if c.Run.ExecutorNodeID == "" || c.Run.ExecutorAddress == "" || c.Run.ShardTotal < 1 {
			return store.ExecutorNode{}, fmt.Errorf("broadcast run is missing fixed executor or shard metadata")
		}
		return store.ExecutorNode{NodeID: c.Run.ExecutorNodeID, Address: c.Run.ExecutorAddress}, nil
	}
	if fixedExecutorForRecovery(c.Run, c.Job) {
		return store.ExecutorNode{NodeID: c.Run.ExecutorNodeID, Address: c.Run.ExecutorAddress}, nil
	}
	if len(c.Run.OverrideAddresses) > 0 {
		requiredLabels, excludedLabels, routeErr = e.store.JobExecutorLabels(ctx, c.Job.ID)
		if routeErr != nil {
			return store.ExecutorNode{}, fmt.Errorf("load executor labels: %w", routeErr)
		}
		strategy, routeErr = e.store.ExecutorRouteStrategy(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
		activeNodes = store.OverrideExecutorNodes(c.Run.OverrideAddresses)
	} else {
		strategy, activeNodes, routeErr = e.store.ExecutorRouteCandidates(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
		activeNodes = store.FilterExecutorNodes(activeNodes, requiredLabels, excludedLabels)
	}
	if routeErr != nil {
		return store.ExecutorNode{}, routeErr
	}
	if strategy == "failover" || strategy == "busyover" {
		candidates := make([]executorCandidate, 0, len(activeNodes))
		for _, candidate := range activeNodes {
			candidates = append(candidates, executorCandidate{ID: candidate.NodeID, Address: candidate.Address})
		}
		selected, selectErr := selectActiveExecutor(ctx, e.client, e.executorGRPC, strategy, c.Job.ID, candidates, time.Second)
		if selectErr != nil {
			return store.ExecutorNode{}, selectErr
		}
		for _, candidate := range activeNodes {
			if candidate.NodeID == selected.ID {
				return candidate, nil
			}
		}
		return store.ExecutorNode{}, fmt.Errorf("selected executor disappeared")
	}
	if len(activeNodes) == 1 {
		return activeNodes[0], nil
	}
	if len(c.Run.OverrideAddresses) == 0 {
		requiredLabels, excludedLabels, routeErr = e.store.JobExecutorLabels(ctx, c.Job.ID)
		if routeErr != nil {
			return store.ExecutorNode{}, fmt.Errorf("load executor labels: %w", routeErr)
		}
	}
	reserve := e.store.ReserveExecutorRoute
	if len(c.Run.OverrideAddresses) > 0 {
		reserve = func(ctx context.Context, tenantID, groupID, jobID string, selector store.ExecutorSelector) (store.ExecutorNode, error) {
			return e.store.ReserveExecutorOverrideRoute(ctx, tenantID, groupID, jobID, c.Run.OverrideAddresses, selector)
		}
	}
	return reserve(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID, func(snapshot store.ExecutorRoutingSnapshot) (store.ExecutorNode, error) {
		if len(c.Run.OverrideAddresses) == 0 {
			snapshot.Nodes = store.FilterExecutorNodes(snapshot.Nodes, requiredLabels, excludedLabels)
		}
		candidates := make([]executorCandidate, 0, len(snapshot.Nodes))
		for _, candidate := range snapshot.Nodes {
			candidates = append(candidates, executorCandidate{ID: candidate.NodeID, Address: candidate.Address, UseCount: candidate.UseCount, LastUsedAt: candidate.LastUsedAt})
		}
		selected, selectErr := selectExecutorNode(candidates, snapshot.Strategy, snapshot.Cursor, uint64(randomUint16()), c.Job.ID)
		if selectErr != nil {
			return store.ExecutorNode{}, selectErr
		}
		for _, candidate := range snapshot.Nodes {
			if candidate.NodeID == selected.ID {
				return candidate, nil
			}
		}
		return store.ExecutorNode{}, fmt.Errorf("selected executor disappeared")
	})
}

func (e *Engine) execute(parent context.Context, c store.ClaimedRun) {
	started := time.Now()
	delay := dispatchDelay(started, c.Run.ScheduledAt)
	observability.DispatchDelay.WithLabelValues(c.Run.TriggerType).Observe(delay.Seconds())
	defer func() { observability.RunDuration.Observe(time.Since(started).Seconds()) }()
	timeout := time.Duration(c.Job.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)
	watchDone := make(chan struct{})
	go e.watchCancellation(ctx, cancel, c.Run.ID, watchDone)
	defer func() {
		cancel()
		<-watchDone
	}()
	if e.executorGRPC == nil {
		e.fail(parent, c, fmt.Errorf("executor gRPC transport is required"))
		return
	}
	if c.Job.ExecutorGroupID == "" {
		e.fail(parent, c, fmt.Errorf("job requires an executor group"))
		return
	}
	callbackToken, tokenHash, err := newCallbackToken()
	if err != nil {
		e.fail(parent, c, err)
		return
	}
	callbackDeadline := time.Now().Add(time.Duration(c.Job.CallbackTimeoutSeconds) * time.Second)
	body := strings.ReplaceAll(c.Job.BodyTemplate, "{{input}}", c.Run.RuntimeInput)
	node, routeErr := e.routeClaimedExecutor(ctx, c)
	if routeErr != nil {
		e.fail(parent, c, routeErr)
		return
	}
	externalExecutionID := c.Run.ExternalExecutionID
	if externalExecutionID == "" {
		externalExecutionID = c.Run.ID
	}
	dispatchRequest := &executorv1.DispatchRequest{RunId: c.Run.ID, ExternalExecutionId: externalExecutionID, JobId: c.Job.ID, Attempt: c.Run.Attempt, Handler: c.Job.ExecutorHandler, Input: c.Run.RuntimeInput, CallbackToken: callbackToken, TimeoutSeconds: c.Job.TimeoutSeconds, BroadcastGroupId: c.Run.BroadcastGroupID, BroadcastIndex: c.Run.ShardIndex, BroadcastTotal: c.Run.ShardTotal, ScriptLanguage: c.Job.ScriptLanguage, ScriptSource: c.Job.ScriptSource}
	if c.Job.DockerRegistryAuth.Configured {
		dispatchRequest.DockerRegistryAuth = &executorv1.DockerRegistryAuth{Server: c.Job.DockerRegistryAuth.Server, Username: c.Job.DockerRegistryAuth.Username, Password: c.Job.DockerRegistryAuth.Password}
	}
	if c.Job.TargetURL != "" {
		headers := maps.Clone(c.Job.Headers)
		if headers == nil {
			headers = make(map[string]string, 4)
		}
		headers["X-Job-Run-ID"] = c.Run.ID
		headers["X-Job-Callback-URL"] = e.publicBaseURL + "/api/v1/callbacks/" + c.Run.ID
		headers["X-Job-Log-URL"] = e.publicBaseURL + "/api/v1/runs/" + c.Run.ID + "/logs"
		headers["X-Job-Callback-Token"] = callbackToken
		dispatchRequest.Http = &executorv1.HttpExecution{Url: c.Job.TargetURL, Method: c.Job.HTTPMethod, Headers: headers, Body: body}
	}
	if c.Job.KubernetesClusterID != "" {
		cluster, clusterErr := e.store.GetKubernetesCluster(ctx, c.Job.TenantID, c.Job.KubernetesClusterID)
		if clusterErr != nil {
			e.fail(parent, c, fmt.Errorf("load kubernetes cluster: %w", clusterErr))
			return
		}
		dispatchRequest.KubernetesCluster = &executorv1.KubernetesCluster{AuthMode: cluster.AuthMode, ApiServer: cluster.APIServer, Namespace: cluster.Namespace, Kubeconfig: cluster.Credentials.Kubeconfig, Token: cluster.Credentials.Token, CaData: cluster.Credentials.CAData, InsecureSkipTlsVerify: cluster.InsecureSkipTLSVerify}
	}
	if err := e.store.PrepareClaimedExecutorDispatch(ctx, c.Run, node.NodeID, node.Address, tokenHash, callbackDeadline); err != nil {
		e.fail(parent, c, err)
		return
	}
	if err := e.executorGRPC.dispatch(ctx, node.Address, dispatchRequest); err != nil {
		if callbackErr := persistWrite(parent, func(ackCtx context.Context) error {
			return e.store.CompleteCallback(ackCtx, c.Run.ID, tokenHash, false, truncateMessage(err.Error(), 4096))
		}); callbackErr != nil {
			slog.Error("complete failed gRPC dispatch", "run_id", c.Run.ID, "dispatch_error", err, "callback_error", callbackErr)
		}
	}
}

func truncateMessage(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func dispatchDelay(startedAt, scheduledAt time.Time) time.Duration {
	delay := startedAt.Sub(scheduledAt)
	if delay < 0 {
		return 0
	}
	return delay
}

func (e *Engine) watchCancellation(ctx context.Context, cancel context.CancelFunc, runID string, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(cancelWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			running, err := e.store.IsRunRunning(ctx, runID)
			if err != nil {
				continue
			}
			if !running {
				cancel()
				return
			}
		}
	}
}
func newCallbackToken() (string, []byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate callback token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	return token, hash[:], nil
}
func (e *Engine) fail(ctx context.Context, c store.ClaimedRun, err error) {
	e.failWithState(ctx, c, classifyRunFailure(err), 0, err.Error())
}
func (e *Engine) failWithState(ctx context.Context, c store.ClaimedRun, state string, statusCode int, message string) {
	var delay *time.Duration
	if shouldRetry(c.Run.Attempt, c.Job.MaxRetries) {
		value := retryDelay(c.Run.Attempt, randomUint16())
		delay = &value
	}
	err := persistWrite(ctx, func(ackCtx context.Context) error {
		_, failErr := e.store.FailRun(ackCtx, c.Run, state, statusCode, message, delay)
		return failErr
	})
	if err != nil {
		slog.Error("fail run", "run_id", c.Run.ID, "error", err)
		return
	}
	observability.Runs.WithLabelValues(state).Inc()
}
func classifyRunFailure(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timed_out"
	}
	return "failed"
}
func fixedExecutorForRecovery(run store.Run, job store.Job) bool {
	return job.ScriptLanguage == "docker" && run.ExecutorAddress != ""
}
func shouldRetry(attempt, maxRetries int32) bool {
	return attempt <= maxRetries
}
func retryDelay(attempt int32, random uint16) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 6 {
		shift = 6
	}
	base := time.Second * time.Duration(1<<shift)
	jitterSlots := uint16(500 << shift)
	return base + time.Duration(random%jitterSlots)*time.Millisecond
}
func randomUint16() uint16 {
	var raw [2]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint16(raw[:])
}
