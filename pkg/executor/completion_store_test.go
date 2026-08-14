package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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
