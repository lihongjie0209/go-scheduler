package core

import (
	"context"
	"fmt"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func (e *Engine) dispatch(ctx context.Context, sem chan struct{}) error {
	available := availableWorkerSlots(sem)
	if available == 0 {
		return nil
	}
	claimStartedAt := time.Now()
	runs, err := e.repository.ClaimRuns(ctx, e.owner, available, 2*time.Minute)
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
		requiredLabels, excludedLabels, routeErr = e.repository.JobExecutorLabels(ctx, c.Job.ID)
		if routeErr != nil {
			return store.ExecutorNode{}, fmt.Errorf("load executor labels: %w", routeErr)
		}
		strategy, routeErr = e.repository.ExecutorRouteStrategy(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
		activeNodes = store.OverrideExecutorNodes(c.Run.OverrideAddresses)
	} else {
		strategy, activeNodes, routeErr = e.repository.ExecutorRouteCandidates(ctx, c.Job.TenantID, c.Job.ExecutorGroupID, c.Job.ID)
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
		requiredLabels, excludedLabels, routeErr = e.repository.JobExecutorLabels(ctx, c.Job.ID)
		if routeErr != nil {
			return store.ExecutorNode{}, fmt.Errorf("load executor labels: %w", routeErr)
		}
	}
	reserve := e.repository.ReserveExecutorRoute
	if len(c.Run.OverrideAddresses) > 0 {
		reserve = func(ctx context.Context, tenantID, groupID, jobID string, selector store.ExecutorSelector) (store.ExecutorNode, error) {
			return e.repository.ReserveExecutorOverrideRoute(ctx, tenantID, groupID, jobID, c.Run.OverrideAddresses, selector)
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
