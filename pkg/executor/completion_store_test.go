package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/protobuf/proto"
)

func TestFileCompletionStorePersistsAcrossInstances(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "completions")
	first, err := NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	record := CompletionRecord{RunID: "run-1", Token: "secret", Succeeded: false, Message: "failed", CreatedAt: time.Now().UTC()}
	if err = first.Save(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(first.path(record.RunID))
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions != completionStoreFileMode {
		t.Fatalf("completion file permissions = %o, want %o", permissions, completionStoreFileMode)
	}
	second, err := NewFileCompletionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	records, err := second.List(t.Context())
	if err != nil || len(records) != 1 || !reflect.DeepEqual(records[0], record) {
		t.Fatalf("reloaded completion records = %+v, %v", records, err)
	}
	if err = second.Delete(t.Context(), record.RunID); err != nil {
		t.Fatal(err)
	}
	if records, err = first.List(t.Context()); err != nil || len(records) != 0 {
		t.Fatalf("completion remained after delete: %+v, %v", records, err)
	}
}

func TestFileCompletionStoreRejectsOverwriteAndUnsafeRunID(t *testing.T) {
	t.Parallel()
	store, err := NewFileCompletionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record := CompletionRecord{RunID: "run-1", Token: "secret", CreatedAt: time.Now().UTC()}
	if err = store.Save(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err = store.Save(t.Context(), record); err == nil {
		t.Fatal("existing completion record was overwritten")
	}
	record.RunID = "../escape"
	if err = store.Save(t.Context(), record); err == nil {
		t.Fatal("unsafe completion run ID was accepted")
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(store.directory), "escape.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe completion escaped state directory: %v", err)
	}
}

func TestFileCompletionStoreHonorsCancelledContext(t *testing.T) {
	t.Parallel()
	store, err := NewFileCompletionStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	record := CompletionRecord{RunID: "run-1", Token: "secret", CreatedAt: time.Now().UTC()}
	if err = store.Save(ctx, record); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save() error = %v, want context canceled", err)
	}
}

func TestFileCompletionStoreEnforcesAndRecoversCapacity(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	store, err := NewFileCompletionStore(directory, FileCompletionStoreOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := CompletionRecord{RunID: "run-1", Token: "secret", CreatedAt: time.Now().UTC()}
	if err = store.Save(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := CompletionRecord{RunID: "run-2", Token: "secret", CreatedAt: time.Now().UTC()}
	if err = store.Save(t.Context(), second); err == nil {
		t.Fatal("completion store accepted a record over capacity")
	}
	restarted, err := NewFileCompletionStore(directory, FileCompletionStoreOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Save(t.Context(), second); err == nil {
		t.Fatal("restarted completion store did not rebuild pending capacity")
	}
	if err = restarted.Delete(t.Context(), first.RunID); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Save(t.Context(), second); err != nil {
		t.Fatalf("completion capacity was not released after delete: %v", err)
	}
}

func TestFileCompletionStoreRemovesInterruptedWriteOnStartup(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	temporary := filepath.Join(directory, ".completion-interrupted")
	if err := os.WriteFile(temporary, []byte("partial"), completionStoreFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileCompletionStore(directory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted completion write remained: %v", err)
	}
}

func TestFileCompletionStorePersistsExecutionsAcrossInstances(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	request := &executorv1.DispatchRequest{RunId: "run-execution", JobId: "job", Handler: "script", CallbackToken: "secret", TimeoutSeconds: 10, Input: "payload", Http: &executorv1.HttpExecution{Headers: map[string]string{"z-last": "value", "a-first": "value"}}}
	first, err := NewFileCompletionStore(directory, FileCompletionStoreOptions{MaxExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = first.SaveExecution(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err = first.SaveExecution(t.Context(), request); err != nil {
		t.Fatalf("identical execution save was not idempotent: %v", err)
	}
	different := proto.Clone(request).(*executorv1.DispatchRequest)
	different.Input = "different"
	if err = first.SaveExecution(t.Context(), different); err == nil {
		t.Fatal("different execution content overwrote existing record")
	}
	if err = first.SaveExecution(t.Context(), &executorv1.DispatchRequest{RunId: "run-2", JobId: "job", Handler: "script", CallbackToken: "secret", TimeoutSeconds: 10}); err == nil {
		t.Fatal("execution capacity was not enforced")
	}
	restarted, err := NewFileCompletionStore(directory, FileCompletionStoreOptions{MaxExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	requests, err := restarted.ListExecutions(t.Context())
	if err != nil || len(requests) != 1 || !proto.Equal(requests[0], request) {
		t.Fatalf("reloaded executions = %+v, %v", requests, err)
	}
	if err = restarted.SaveExecution(t.Context(), requests[0]); err != nil {
		t.Fatalf("decoded execution with map fields was not idempotent: %v", err)
	}
	if err = restarted.DeleteExecution(t.Context(), request.GetRunId()); err != nil {
		t.Fatal(err)
	}
	if requests, err = restarted.ListExecutions(t.Context()); err != nil || len(requests) != 0 {
		t.Fatalf("execution remained after delete: %+v, %v", requests, err)
	}
}
