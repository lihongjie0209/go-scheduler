package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type fakeRunRepository struct {
	run           store.Run
	cancelReason  string
	callbackHash  []byte
	cancelCalls   int
	callbackCalls int
}

func (r *fakeRunRepository) ListRunsFiltered(context.Context, string, string, string, int) ([]store.Run, error) {
	return []store.Run{r.run}, nil
}

func (r *fakeRunRepository) GetRun(context.Context, string, string) (store.Run, error) {
	return r.run, nil
}

func (r *fakeRunRepository) CancelRun(_ context.Context, _, _, reason string) (store.Run, error) {
	r.cancelCalls++
	r.cancelReason = reason
	return r.run, nil
}

func (r *fakeRunRepository) CompleteCallback(_ context.Context, _ string, hash []byte, _ bool, _ string) error {
	r.callbackCalls++
	r.callbackHash = append([]byte(nil), hash...)
	return nil
}

type fakeRunCanceller struct {
	calls   int
	address string
	runID   string
	reason  string
}

func (c *fakeRunCanceller) Cancel(_ context.Context, address, runID, reason string) error {
	c.calls++
	c.address = address
	c.runID = runID
	c.reason = reason
	return nil
}

func TestRunService_Cancel(t *testing.T) {
	t.Parallel()
	repository := &fakeRunRepository{run: store.Run{ID: "run", ExecutorAddress: "grpc://worker:9999"}}
	executor := &fakeRunCanceller{}
	service := NewRunService(repository, repository, executor, nil)

	run, err := service.Cancel(t.Context(), "tenant", "run", " ")
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "run" || repository.cancelCalls != 1 || repository.cancelReason != "cancelled by operator" {
		t.Fatalf("run = %+v, cancel calls = %d, reason = %q", run, repository.cancelCalls, repository.cancelReason)
	}
	if executor.calls != 1 || executor.address != "grpc://worker:9999" || executor.runID != "run" || executor.reason != repository.cancelReason {
		t.Fatalf("executor = %+v", executor)
	}
}

func TestRunService_RejectsInvalidCancel(t *testing.T) {
	t.Parallel()
	repository := &fakeRunRepository{}
	service := NewRunService(repository, repository, nil, nil)

	_, err := service.Cancel(t.Context(), "", "run", "reason")
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || repository.cancelCalls != 0 {
		t.Fatalf("error = %v, cancel calls = %d", err, repository.cancelCalls)
	}
}

func TestRunService_CompleteCallback(t *testing.T) {
	t.Parallel()
	repository := &fakeRunRepository{}
	notifications := 0
	service := NewRunService(repository, repository, nil, func() { notifications++ })

	if err := service.CompleteCallback(t.Context(), "run", "secret-token", true, "done"); err != nil {
		t.Fatal(err)
	}
	expectedHash := sha256.Sum256([]byte("secret-token"))
	if repository.callbackCalls != 1 || string(repository.callbackHash) != string(expectedHash[:]) || notifications != 1 {
		t.Fatalf("callback calls = %d, hash = %x, notifications = %d", repository.callbackCalls, repository.callbackHash, notifications)
	}
}

var _ RunReader = (*fakeRunRepository)(nil)
var _ RunWriter = (*fakeRunRepository)(nil)
var _ RunCanceller = (*fakeRunCanceller)(nil)
