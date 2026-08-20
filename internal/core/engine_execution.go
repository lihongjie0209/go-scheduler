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
	"strings"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

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
		cluster, clusterErr := e.repository.GetKubernetesCluster(ctx, c.Job.TenantID, c.Job.KubernetesClusterID)
		if clusterErr != nil {
			e.fail(parent, c, fmt.Errorf("load kubernetes cluster: %w", clusterErr))
			return
		}
		dispatchRequest.KubernetesCluster = &executorv1.KubernetesCluster{AuthMode: cluster.AuthMode, ApiServer: cluster.APIServer, Namespace: cluster.Namespace, Kubeconfig: cluster.Credentials.Kubeconfig, Token: cluster.Credentials.Token, CaData: cluster.Credentials.CAData, InsecureSkipTlsVerify: cluster.InsecureSkipTLSVerify}
	}
	if err := e.repository.PrepareClaimedExecutorDispatch(ctx, c.Run, node.NodeID, node.Address, tokenHash, callbackDeadline); err != nil {
		e.fail(parent, c, err)
		return
	}
	if err := e.executorGRPC.dispatch(ctx, node.Address, dispatchRequest); err != nil {
		if callbackErr := persistWrite(parent, func(ackCtx context.Context) error {
			return e.repository.CompleteCallback(ackCtx, c.Run.ID, tokenHash, false, truncateMessage(err.Error(), 4096))
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
			running, err := e.repository.IsRunRunning(ctx, runID)
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
		_, failErr := e.repository.FailRun(ackCtx, c.Run, state, statusCode, message, delay)
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
