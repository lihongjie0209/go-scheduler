package executor

import (
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
)

var completionRunIDPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

const (
	completionStoreDirectoryMode = 0o700
	completionStoreFileMode      = 0o600
	maxCompletionRecordBytes     = 16 << 10
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

type FileCompletionStore struct {
	directory string
	mu        sync.Mutex
	pending   int
	max       int
}

type FileCompletionStoreOptions struct {
	MaxRecords int
}

func NewFileCompletionStore(directory string, options ...FileCompletionStoreOptions) (*FileCompletionStore, error) {
	configuration := FileCompletionStoreOptions{MaxRecords: 10_000}
	if len(options) > 1 {
		return nil, errors.New("at most one completion store options value is supported")
	}
	if len(options) == 1 {
		configuration = options[0]
	}
	if configuration.MaxRecords < 1 {
		return nil, errors.New("maximum completion records must be positive")
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
	pending := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".completion-") {
			if err = os.Remove(filepath.Join(directory, entry.Name())); err != nil { // #nosec G304 -- names come from the configured directory listing.
				return nil, fmt.Errorf("remove interrupted completion write %q: %w", entry.Name(), err)
			}
			continue
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			pending++
		}
	}
	return &FileCompletionStore{directory: directory, pending: pending, max: configuration.MaxRecords}, nil
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

func (s *FileCompletionStore) path(runID string) string {
	return filepath.Join(s.directory, completionFileName(runID))
}

func completionFileName(runID string) string { return runID + ".json" }

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
