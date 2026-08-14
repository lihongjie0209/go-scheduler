package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/protobuf/proto"
)

var completionRunIDPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const (
	completionStoreDirectoryMode = 0o700
	completionStoreFileMode      = 0o600
	maxCompletionRecordBytes     = 16 << 10
	maxExecutionRecordBytes      = 4 << 20
)

type CompletionRecord struct {
	RunID     string    `json:"run_id"`
	Token     string    `json:"token"`
	Succeeded bool      `json:"succeeded"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type CompletionStore interface {
	Save(context.Context, CompletionRecord) error
	List(context.Context) ([]CompletionRecord, error)
	Delete(context.Context, string) error
}

type ExecutionStore interface {
	CompletionStore
	SaveExecution(context.Context, *executorv1.DispatchRequest) error
	ListExecutions(context.Context) ([]*executorv1.DispatchRequest, error)
	DeleteExecution(context.Context, string) error
}

type FileCompletionStore struct {
	directory     string
	mu            sync.Mutex
	pending       int
	max           int
	executions    int
	maxExecutions int
}

type FileCompletionStoreOptions struct {
	MaxRecords    int
	MaxExecutions int
}

func NewFileCompletionStore(directory string, options ...FileCompletionStoreOptions) (*FileCompletionStore, error) {
	configuration := FileCompletionStoreOptions{MaxRecords: 10_000, MaxExecutions: 1024}
	if len(options) > 1 {
		return nil, errors.New("at most one completion store options value is supported")
	}
	if len(options) == 1 {
		if options[0].MaxRecords != 0 {
			configuration.MaxRecords = options[0].MaxRecords
		}
		if options[0].MaxExecutions != 0 {
			configuration.MaxExecutions = options[0].MaxExecutions
		}
	}
	if configuration.MaxRecords < 1 || configuration.MaxExecutions < 1 {
		return nil, errors.New("maximum completion and execution records must be positive")
	}
	directory = filepath.Clean(strings.TrimSpace(directory))
	if directory == "." || directory == "" {
		return nil, errors.New("completion state directory is required")
	}
	if err := os.MkdirAll(directory, completionStoreDirectoryMode); err != nil {
		return nil, fmt.Errorf("create completion state directory: %w", err)
	}
	if err := os.Chmod(directory, completionStoreDirectoryMode); err != nil {
		return nil, fmt.Errorf("secure completion state directory: %w", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("inspect completion state directory: %w", err)
	}
	pending, executions := 0, 0
	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasPrefix(entry.Name(), ".completion-") || strings.HasPrefix(entry.Name(), ".execution-")) {
			if err = os.Remove(filepath.Join(directory, entry.Name())); err != nil { // #nosec G304 -- names come from the configured directory listing.
				return nil, fmt.Errorf("remove interrupted completion write %q: %w", entry.Name(), err)
			}
			continue
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			pending++
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".execution.pb") {
			executions++
		}
	}
	if pending > configuration.MaxRecords || executions > configuration.MaxExecutions {
		return nil, fmt.Errorf("executor state exceeds configured capacity: completions=%d/%d executions=%d/%d", pending, configuration.MaxRecords, executions, configuration.MaxExecutions)
	}
	store := &FileCompletionStore{directory: directory, pending: pending, max: configuration.MaxRecords, executions: executions, maxExecutions: configuration.MaxExecutions}
	if _, err = store.List(context.Background()); err != nil {
		return nil, err
	}
	if _, err = store.ListExecutions(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileCompletionStore) Save(ctx context.Context, record CompletionRecord) error {
	if err := validateCompletionRecord(record); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode completion record: %w", err)
	}
	if len(raw) > maxCompletionRecordBytes {
		return errors.New("completion record exceeds size limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending >= s.max {
		return fmt.Errorf("completion state contains %d records; maximum is %d", s.pending, s.max)
	}
	if _, err := os.Stat(s.path(record.RunID)); err == nil {
		return fmt.Errorf("completion record for run %q already exists", record.RunID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect existing completion record: %w", err)
	}
	temporary, err := os.CreateTemp(s.directory, ".completion-*")
	if err != nil {
		return fmt.Errorf("create temporary completion record: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}
	if err = temporary.Chmod(completionStoreFileMode); err != nil {
		cleanup()
		return fmt.Errorf("secure temporary completion record: %w", err)
	}
	if _, err = temporary.Write(raw); err != nil {
		cleanup()
		return fmt.Errorf("write temporary completion record: %w", err)
	}
	if err = temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temporary completion record: %w", err)
	}
	if err = temporary.Close(); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("close temporary completion record: %w", err)
	}
	if err = os.Rename(temporaryName, s.path(record.RunID)); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("publish completion record: %w", err)
	}
	s.pending++
	directory, err := os.Open(s.directory) // #nosec G304 -- directory is trusted operator configuration.
	if err != nil {
		return fmt.Errorf("open completion state directory: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("sync completion state directory: %w", err)
	}
	return nil
}

func (s *FileCompletionStore) List(ctx context.Context) ([]CompletionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("list completion records: %w", err)
	}
	records := make([]CompletionRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil, fmt.Errorf("inspect completion record %q: %w", entry.Name(), infoErr)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("completion record %q is not a regular file", entry.Name())
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		raw, readErr := os.ReadFile(filepath.Join(s.directory, entry.Name())) // #nosec G304 -- names come from the configured directory listing.
		if readErr != nil {
			return nil, fmt.Errorf("read completion record %q: %w", entry.Name(), readErr)
		}
		if len(raw) > maxCompletionRecordBytes {
			return nil, fmt.Errorf("completion record %q exceeds size limit", entry.Name())
		}
		var record CompletionRecord
		if unmarshalErr := json.Unmarshal(raw, &record); unmarshalErr != nil {
			return nil, fmt.Errorf("decode completion record %q: %w", entry.Name(), unmarshalErr)
		}
		if validateErr := validateCompletionRecord(record); validateErr != nil {
			return nil, fmt.Errorf("validate completion record %q: %w", entry.Name(), validateErr)
		}
		if entry.Name() != completionFileName(record.RunID) {
			return nil, fmt.Errorf("completion record %q has mismatched run ID", entry.Name())
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].RunID < records[j].RunID
		}
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	return records, nil
}

func (s *FileCompletionStore) Delete(ctx context.Context, runID string) error {
	if err := validateCompletionRunID(runID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Remove the execution inbox first. If the process stops between these
	// removals, replaying an already accepted completion is safe; resurrecting
	// an execution after Core accepted its completion is not.
	if err := s.removeExecutionLocked(runID); err != nil {
		return err
	}
	removed := false
	if err := os.Remove(s.path(runID)); err == nil {
		removed = true
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete completion record: %w", err)
	}
	if removed && s.pending > 0 {
		s.pending--
	}
	directory, err := os.Open(s.directory) // #nosec G304 -- directory is trusted operator configuration.
	if err != nil {
		return fmt.Errorf("open completion state directory: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("sync completion state directory: %w", err)
	}
	return nil
}

func (s *FileCompletionStore) SaveExecution(ctx context.Context, request *executorv1.DispatchRequest) error {
	if request == nil {
		return errors.New("execution request is required")
	}
	if err := validateCompletionRunID(request.GetRunId()); err != nil {
		return err
	}
	if request.GetJobId() == "" || request.GetHandler() == "" || request.GetCallbackToken() == "" || request.GetTimeoutSeconds() < 1 {
		return errors.New("execution request is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return fmt.Errorf("encode execution record: %w", err)
	}
	if len(raw) > maxExecutionRecordBytes {
		return errors.New("execution record exceeds size limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.executionPath(request.GetRunId())
	// #nosec G304 -- target is derived from the configured state directory and a validated run ID.
	if existing, readErr := os.ReadFile(target); readErr == nil {
		if bytes.Equal(existing, raw) {
			return nil
		}
		legacy := new(executorv1.DispatchRequest)
		if unmarshalErr := proto.Unmarshal(existing, legacy); unmarshalErr == nil && legacy.GetExecutionDeadlineUnixMilli() == 0 {
			upgraded := proto.Clone(request).(*executorv1.DispatchRequest)
			upgraded.ExecutionDeadlineUnixMilli = 0
			if proto.Equal(legacy, upgraded) {
				return s.writeAtomicLocked(target, ".execution-*", raw)
			}
		}
		return fmt.Errorf("execution record for run %q already exists with different content", request.GetRunId())
	} else if !errors.Is(readErr, fs.ErrNotExist) {
		return fmt.Errorf("inspect existing execution record: %w", readErr)
	}
	if s.executions >= s.maxExecutions {
		return fmt.Errorf("execution state contains %d records; maximum is %d", s.executions, s.maxExecutions)
	}
	if err = s.writeAtomicLocked(target, ".execution-*", raw); err != nil {
		return err
	}
	s.executions++
	return nil
}

func (s *FileCompletionStore) ListExecutions(ctx context.Context) ([]*executorv1.DispatchRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("list execution records: %w", err)
	}
	requests := make([]*executorv1.DispatchRequest, 0, s.executions)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".execution.pb") {
			continue
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		info, infoErr := entry.Info()
		if infoErr != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("execution record %q is not a readable regular file", entry.Name())
		}
		raw, readErr := os.ReadFile(filepath.Join(s.directory, entry.Name())) // #nosec G304 -- names come from the configured directory listing.
		if readErr != nil {
			return nil, fmt.Errorf("read execution record %q: %w", entry.Name(), readErr)
		}
		if len(raw) > maxExecutionRecordBytes {
			return nil, fmt.Errorf("execution record %q exceeds size limit", entry.Name())
		}
		request := new(executorv1.DispatchRequest)
		if unmarshalErr := proto.Unmarshal(raw, request); unmarshalErr != nil {
			return nil, fmt.Errorf("decode execution record %q: %w", entry.Name(), unmarshalErr)
		}
		if err = validateCompletionRunID(request.GetRunId()); err != nil || entry.Name() != executionFileName(request.GetRunId()) {
			return nil, fmt.Errorf("execution record %q has invalid or mismatched run ID", entry.Name())
		}
		requests = append(requests, request)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].GetRunId() < requests[j].GetRunId() })
	return requests, nil
}

func (s *FileCompletionStore) DeleteExecution(ctx context.Context, runID string) error {
	if err := validateCompletionRunID(runID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.removeExecutionLocked(runID); err != nil {
		return err
	}
	return s.syncDirectoryLocked()
}

func (s *FileCompletionStore) removeExecutionLocked(runID string) error {
	removed := false
	if err := os.Remove(s.executionPath(runID)); err == nil {
		removed = true
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete execution record: %w", err)
	}
	if removed && s.executions > 0 {
		s.executions--
	}
	return nil
}

func (s *FileCompletionStore) writeAtomicLocked(target, pattern string, raw []byte) error {
	temporary, err := os.CreateTemp(s.directory, pattern)
	if err != nil {
		return fmt.Errorf("create temporary executor state: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}
	if err = temporary.Chmod(completionStoreFileMode); err != nil {
		cleanup()
		return fmt.Errorf("secure temporary executor state: %w", err)
	}
	if _, err = temporary.Write(raw); err != nil {
		cleanup()
		return fmt.Errorf("write temporary executor state: %w", err)
	}
	if err = temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temporary executor state: %w", err)
	}
	if err = temporary.Close(); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("close temporary executor state: %w", err)
	}
	if err = os.Rename(temporaryName, target); err != nil {
		_ = os.Remove(temporaryName)
		return fmt.Errorf("publish executor state: %w", err)
	}
	return s.syncDirectoryLocked()
}

func (s *FileCompletionStore) syncDirectoryLocked() error {
	directory, err := os.Open(s.directory) // #nosec G304 -- directory is trusted operator configuration.
	if err != nil {
		return fmt.Errorf("open executor state directory: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("sync executor state directory: %w", err)
	}
	return nil
}

func (s *FileCompletionStore) path(runID string) string {
	return filepath.Join(s.directory, completionFileName(runID))
}

func (s *FileCompletionStore) executionPath(runID string) string {
	return filepath.Join(s.directory, executionFileName(runID))
}

func completionFileName(runID string) string { return runID + ".json" }
func executionFileName(runID string) string  { return runID + ".execution.pb" }

func validateCompletionRecord(record CompletionRecord) error {
	if err := validateCompletionRunID(record.RunID); err != nil {
		return err
	}
	if record.Token == "" || len(record.Token) > 4096 {
		return errors.New("completion token must be between 1 and 4096 bytes")
	}
	if len(record.Message) > 4096 {
		return errors.New("completion message exceeds 4096 bytes")
	}
	if record.CreatedAt.IsZero() {
		return errors.New("completion creation time is required")
	}
	return nil
}

func validateCompletionRunID(runID string) error {
	if runID == "" || len(runID) > 128 || !completionRunIDPattern.MatchString(runID) {
		return errors.New("completion run ID must contain only letters, digits, dots, underscores, or hyphens")
	}
	return nil
}
