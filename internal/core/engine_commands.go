package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

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
	commands, err := e.repository.ClaimExecutorCommands(ctx, e.owner, executorCommandBatchSize)
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
				cluster, clusterErr := e.repository.GetKubernetesCluster(commandCtx, command.TenantID, command.KubernetesClusterID)
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
					return e.repository.CompleteExecutorCommand(ackCtx, e.owner, command.ID)
				}); completeErr != nil && !errors.Is(completeErr, store.ErrConflict) {
					slog.Error("complete executor command", "command_id", command.ID, "run_id", command.RunID, "error", completeErr)
				}
				return
			}
			delay := executorCommandRetryDelay(command.Attempts)
			if retryErr := persistWrite(ctx, func(ackCtx context.Context) error {
				return e.repository.RetryExecutorCommand(ackCtx, e.owner, command.ID, deliverErr.Error(), delay)
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
