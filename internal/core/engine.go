package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	if time.Since(e.lastPartitionRun) >= time.Hour {
		result, err := e.store.MaintainRunPartitions(ctx, time.Now(), e.historyRetention)
		if err != nil {
			slog.Error("partition maintenance failed", "error", err)
		} else {
			slog.Info("partition maintenance completed", "backend", result.Backend, "dropped", result.Dropped)
		}
		e.lastPartitionRun = time.Now()
	}
	if err := e.store.EnqueueDue(ctx, 500); err != nil {
		return fmt.Errorf("enqueue due jobs: %w", err)
	}
	if err := e.store.ExpireCallbacks(ctx); err != nil {
		return fmt.Errorf("expire callbacks: %w", err)
	}
	if time.Since(e.lastCleanup) >= auxiliaryHistoryCleanupInterval {
		if err := e.store.CleanupAuxiliaryHistory(ctx, e.historyRetention); err != nil {
			return fmt.Errorf("cleanup auxiliary history: %w", err)
		}
		e.lastCleanup = time.Now()
	}
	if time.Since(e.lastRunCleanup) >= time.Hour {
		if err := e.store.CleanupRunHistory(ctx, e.historyRetention); err != nil {
			return fmt.Errorf("cleanup run history: %w", err)
		}
		e.lastRunCleanup = time.Now()
	}
	available := availableWorkerSlots(sem)
	if available == 0 {
		observability.WorkerSaturationTicks.Inc()
		return nil
	}
	return e.dispatch(ctx, sem)
}

func (e *Engine) dispatch(ctx context.Context, sem chan struct{}) error {
	available := availableWorkerSlots(sem)
	if available == 0 {
		return nil
	}
	runs, err := e.store.ClaimRuns(ctx, e.owner, available, 2*time.Minute)
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
	callbackToken, tokenHash, err := newCallbackToken()
	if err != nil {
		e.fail(parent, c, err)
		return
	}
	callbackURL := e.publicBaseURL + "/api/v1/callbacks/" + c.Run.ID
	logURL := e.publicBaseURL + "/api/v1/runs/" + c.Run.ID + "/logs"
	callbackDeadline := time.Now().Add(time.Duration(c.Job.CallbackTimeoutSeconds) * time.Second)
	if e.executorGRPC == nil {
		if err = e.store.ActivateRunToken(ctx, c.Run.ID, tokenHash, callbackDeadline); err != nil {
			e.fail(parent, c, fmt.Errorf("activate run token: %w", err))
			return
		}
	}
	targetURL := c.Job.TargetURL
	method := c.Job.HTTPMethod
	body := strings.ReplaceAll(c.Job.BodyTemplate, "{{input}}", c.Run.RuntimeInput)
	if c.Job.ExecutorGroupID != "" {
		var requiredLabels, excludedLabels []string
		var node store.ExecutorNode
		var routeErr error
		strategy := ""
		var activeNodes []store.ExecutorNode
		if c.Run.BroadcastGroupID != "" {
			if c.Run.ExecutorNodeID == "" || c.Run.ExecutorAddress == "" || c.Run.ShardTotal < 1 {
				routeErr = fmt.Errorf("broadcast run is missing fixed executor or shard metadata")
			} else {
				node = store.ExecutorNode{NodeID: c.Run.ExecutorNodeID, Address: c.Run.ExecutorAddress}
			}
		} else if len(c.Run.OverrideAddresses) > 0 {
			var labelErr error
			requiredLabels, excludedLabels, labelErr = e.store.JobExecutorLabels(ctx, c.Job.ID)
			if labelErr != nil {
				e.fail(parent, c, fmt.Errorf("load executor labels: %w", labelErr))
				return
			}
			strategy, routeErr = e.store.ExecutorRouteStrategy(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
			activeNodes = store.OverrideExecutorNodes(c.Run.OverrideAddresses)
		} else {
			strategy, activeNodes, routeErr = e.store.ExecutorRouteCandidates(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
			activeNodes = store.FilterExecutorNodes(activeNodes, requiredLabels, excludedLabels)
		}
		if c.Run.BroadcastGroupID == "" && routeErr == nil && (strategy == "failover" || strategy == "busyover") {
			candidates := make([]executorCandidate, 0, len(activeNodes))
			for _, candidate := range activeNodes {
				candidates = append(candidates, executorCandidate{ID: candidate.NodeID, Address: candidate.Address})
			}
			selected, selectErr := selectActiveExecutor(ctx, e.client, strategy, c.Job.ID, candidates, time.Second)
			if selectErr != nil {
				routeErr = selectErr
			} else {
				for _, candidate := range activeNodes {
					if candidate.NodeID == selected.ID {
						node = candidate
						break
					}
				}
			}
		} else if c.Run.BroadcastGroupID == "" && routeErr == nil && len(activeNodes) == 1 {
			node = activeNodes[0]
		} else if c.Run.BroadcastGroupID == "" && routeErr == nil {
			if len(c.Run.OverrideAddresses) == 0 {
				var labelErr error
				requiredLabels, excludedLabels, labelErr = e.store.JobExecutorLabels(ctx, c.Job.ID)
				if labelErr != nil {
					routeErr = fmt.Errorf("load executor labels: %w", labelErr)
				}
			}
			if routeErr != nil {
				e.fail(parent, c, routeErr)
				return
			}
			reserve := e.store.ReserveExecutorRoute
			if len(c.Run.OverrideAddresses) > 0 {
				reserve = func(ctx context.Context, tenantID, groupID, jobID string, selector store.ExecutorSelector) (store.ExecutorNode, error) {
					return e.store.ReserveExecutorOverrideRoute(ctx, tenantID, groupID, jobID, c.Run.OverrideAddresses, selector)
				}
			}
			node, routeErr = reserve(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID, func(snapshot store.ExecutorRoutingSnapshot) (store.ExecutorNode, error) {
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
		if routeErr != nil {
			e.fail(parent, c, routeErr)
			return
		}
		if e.executorGRPC != nil {
			dispatchRequest := &executorv1.DispatchRequest{RunId: c.Run.ID, JobId: c.Job.ID, Attempt: c.Run.Attempt, Handler: c.Job.ExecutorHandler, Input: c.Run.RuntimeInput, CallbackToken: callbackToken, TimeoutSeconds: c.Job.TimeoutSeconds, BroadcastGroupId: c.Run.BroadcastGroupID, BroadcastIndex: c.Run.ShardIndex, BroadcastTotal: c.Run.ShardTotal, ScriptLanguage: c.Job.ScriptLanguage, ScriptSource: c.Job.ScriptSource}
			if c.Job.TargetURL != "" {
				dispatchRequest.Http = &executorv1.HttpExecution{Url: c.Job.TargetURL, Method: c.Job.HTTPMethod, Headers: c.Job.Headers, Body: body}
			}
			if c.Job.KubernetesClusterID != "" {
				cluster, clusterErr := e.store.GetKubernetesCluster(ctx, c.Job.TenantID, c.Job.KubernetesClusterID)
				if clusterErr != nil {
					e.fail(parent, c, fmt.Errorf("load kubernetes cluster: %w", clusterErr))
					return
				}
				executionID, executionErr := e.store.RootRunID(ctx, c.Job.TenantID, c.Run.ID)
				if executionErr != nil {
					e.fail(parent, c, fmt.Errorf("resolve external execution identity: %w", executionErr))
					return
				}
				dispatchRequest.ExternalExecutionId = executionID
				dispatchRequest.KubernetesCluster = &executorv1.KubernetesCluster{AuthMode: cluster.AuthMode, ApiServer: cluster.APIServer, Namespace: cluster.Namespace, Kubeconfig: cluster.Credentials.Kubeconfig, Token: cluster.Credentials.Token, CaData: cluster.Credentials.CAData, InsecureSkipTlsVerify: cluster.InsecureSkipTLSVerify}
			}
			if err := e.store.PrepareExecutorDispatch(ctx, c.Run.ID, node.NodeID, node.Address, tokenHash, callbackDeadline); err != nil {
				e.fail(parent, c, err)
				return
			}
			if err := e.executorGRPC.dispatch(ctx, node.Address, dispatchRequest); err != nil {
				if callbackErr := e.store.CompleteCallback(parent, c.Run.ID, tokenHash, false, truncateMessage(err.Error(), 4096)); callbackErr != nil {
					slog.Error("complete failed gRPC dispatch", "run_id", c.Run.ID, "dispatch_error", err, "callback_error", callbackErr)
				}
			}
			return
		}
		if routeErr = e.store.AssignRunExecutor(ctx, c.Run.ID, node.NodeID, node.Address); routeErr != nil {
			e.fail(parent, c, fmt.Errorf("assign executor: %w", routeErr))
			return
		}
		targetURL = strings.TrimRight(node.Address, "/") + "/run"
		method = http.MethodPost
		runPayload := map[string]any{"run_id": c.Run.ID, "job_id": c.Job.ID, "handler": c.Job.ExecutorHandler, "input": c.Run.RuntimeInput, "callback_url": callbackURL, "log_url": logURL, "callback_token": callbackToken, "timeout_seconds": c.Job.TimeoutSeconds, "broadcast_group_id": c.Run.BroadcastGroupID, "broadcast_index": c.Run.ShardIndex, "broadcast_total": c.Run.ShardTotal, "script_language": c.Job.ScriptLanguage, "script_source": c.Job.ScriptSource}
		if c.Job.KubernetesClusterID != "" {
			cluster, clusterErr := e.store.GetKubernetesCluster(ctx, c.Job.TenantID, c.Job.KubernetesClusterID)
			if clusterErr != nil {
				e.fail(parent, c, fmt.Errorf("load kubernetes cluster: %w", clusterErr))
				return
			}
			executionID, executionErr := e.store.RootRunID(ctx, c.Job.TenantID, c.Run.ID)
			if executionErr != nil {
				e.fail(parent, c, fmt.Errorf("resolve external execution identity: %w", executionErr))
				return
			}
			runPayload["external_execution_id"] = executionID
			runPayload["kubernetes_cluster"] = map[string]any{"auth_mode": cluster.AuthMode, "api_server": cluster.APIServer, "namespace": cluster.Namespace, "kubeconfig": cluster.Credentials.Kubeconfig, "token": cluster.Credentials.Token, "ca_data": cluster.Credentials.CAData, "insecure_skip_tls_verify": cluster.InsecureSkipTLSVerify}
		}
		payload, marshalErr := json.Marshal(runPayload)
		if marshalErr != nil {
			e.fail(parent, c, marshalErr)
			return
		}
		body = string(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewBufferString(body))
	if err != nil {
		e.fail(parent, c, err)
		return
	}
	for k, v := range c.Job.Headers {
		req.Header.Set(k, v)
	}
	if c.Job.ExecutorGroupID != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Job-Run-ID", c.Run.ID)
	req.Header.Set("X-Job-Callback-URL", callbackURL)
	req.Header.Set("X-Job-Log-URL", logURL)
	req.Header.Set("X-Job-Callback-Token", callbackToken)
	resp, err := e.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			running, stateErr := e.store.IsRunRunning(parent, c.Run.ID)
			if stateErr == nil && !running {
				return
			}
		}
		e.fail(parent, c, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		e.fail(parent, c, readErr)
		return
	}
	commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancelCommit()
	if resp.StatusCode == http.StatusAccepted {
		if err := e.store.MarkWaitingCallback(commitCtx, c.Run.ID, resp.StatusCode, tokenHash, callbackDeadline); err != nil {
			slog.Error("mark waiting callback", "run_id", c.Run.ID, "error", err)
		}
		return
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := e.store.CompleteRun(commitCtx, c.Run, true, resp.StatusCode, string(payload), ""); err != nil {
			slog.Error("complete run", "run_id", c.Run.ID, "error", err)
		}
		observability.Runs.WithLabelValues("succeeded").Inc()
		return
	}
	e.failWithStatus(commitCtx, c, resp.StatusCode, string(payload))
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
	ticker := time.NewTicker(200 * time.Millisecond)
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
func (e *Engine) failWithStatus(ctx context.Context, c store.ClaimedRun, statusCode int, message string) {
	e.failWithState(ctx, c, "failed", statusCode, message)
}
func (e *Engine) failWithState(ctx context.Context, c store.ClaimedRun, state string, statusCode int, message string) {
	var delay *time.Duration
	if shouldRetry(c.Run.Attempt, c.Job.MaxRetries) {
		value := retryDelay(c.Run.Attempt, randomUint16())
		delay = &value
	}
	if _, err := e.store.FailRun(ctx, c.Run, state, statusCode, message, delay); err != nil {
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
