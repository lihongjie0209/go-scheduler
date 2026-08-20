package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type RunReader interface {
	ListRunsFiltered(context.Context, string, string, string, int) ([]store.Run, error)
	GetRun(context.Context, string, string) (store.Run, error)
}

type RunWriter interface {
	CancelRun(context.Context, string, string, string) (store.Run, error)
	CompleteCallback(context.Context, string, []byte, bool, string) error
}

type RunCanceller interface {
	Cancel(context.Context, string, string, string) error
}

type RunService struct {
	reader     RunReader
	writer     RunWriter
	executor   RunCanceller
	onTerminal func()
}

func NewRunService(reader RunReader, writer RunWriter, executor RunCanceller, onTerminal func()) *RunService {
	return &RunService{reader: reader, writer: writer, executor: executor, onTerminal: onTerminal}
}

func (s *RunService) List(ctx context.Context, tenantID, jobID, broadcastGroupID string, limit int) ([]store.Run, error) {
	return s.reader.ListRunsFiltered(ctx, tenantID, jobID, broadcastGroupID, limit)
}

func (s *RunService) Get(ctx context.Context, tenantID, runID string) (store.Run, error) {
	if tenantID == "" || runID == "" {
		return store.Run{}, &ValidationError{err: fmt.Errorf("tenant_id and run_id are required")}
	}
	return s.reader.GetRun(ctx, tenantID, runID)
}

func (s *RunService) Cancel(ctx context.Context, tenantID, runID, reason string) (store.Run, error) {
	if tenantID == "" || runID == "" {
		return store.Run{}, &ValidationError{err: fmt.Errorf("tenant_id and run_id are required")}
	}
	normalizedReason, err := normalizeCancelReason(reason)
	if err != nil {
		return store.Run{}, &ValidationError{err: err}
	}
	run, err := s.writer.CancelRun(ctx, tenantID, runID, normalizedReason)
	if err != nil {
		return store.Run{}, err
	}
	if s.executor != nil && run.ExecutorAddress != "" {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		cancelErr := s.executor.Cancel(cancelCtx, run.ExecutorAddress, run.ID, normalizedReason)
		cancel()
		if cancelErr != nil {
			slog.Warn("cancel executor run failed", "run_id", run.ID, "executor_address", run.ExecutorAddress, "error", cancelErr)
		}
	}
	return run, nil
}

func (s *RunService) CompleteCallback(ctx context.Context, runID, token string, succeeded bool, message string) error {
	if runID == "" || token == "" {
		return &ValidationError{err: fmt.Errorf("run_id and token are required")}
	}
	hash := sha256.Sum256([]byte(token))
	if err := s.writer.CompleteCallback(ctx, runID, hash[:], succeeded, message); err != nil {
		return err
	}
	if s.onTerminal != nil {
		s.onTerminal()
	}
	return nil
}

func normalizeCancelReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "cancelled by operator"
	}
	if len(reason) > 500 {
		return "", fmt.Errorf("reason must not exceed 500 bytes")
	}
	return reason, nil
}
