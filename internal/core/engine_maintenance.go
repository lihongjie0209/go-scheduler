package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/observability"
)

func (e *Engine) tick(ctx context.Context, sem chan struct{}) error {
	if err := e.repository.EnqueueDue(ctx, 500); err != nil {
		return fmt.Errorf("enqueue due jobs: %w", err)
	}
	if err := e.repository.ExpireCallbacks(ctx); err != nil {
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
		result, err := e.repository.MaintainRunPartitions(ctx, time.Now(), e.historyRetention)
		if err != nil {
			slog.Error("partition maintenance failed", "error", err)
		} else {
			slog.Info("partition maintenance completed", "backend", result.Backend, "dropped", result.Dropped)
			e.lastPartitionRun = time.Now()
		}
	}
	if time.Since(e.lastCleanup) >= auxiliaryHistoryCleanupInterval {
		if err := e.repository.CleanupAuxiliaryHistory(ctx, e.historyRetention); err != nil {
			slog.Error("cleanup auxiliary history", "error", err)
		} else {
			e.lastCleanup = time.Now()
		}
	}
	if time.Since(e.lastRunCleanup) >= time.Hour {
		if err := e.repository.CleanupRunHistory(ctx, e.historyRetention); err != nil {
			slog.Error("cleanup run history", "error", err)
		} else {
			e.lastRunCleanup = time.Now()
		}
	}
}
