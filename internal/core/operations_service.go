package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type RunLogRepository interface {
	AppendRunLogs(context.Context, string, []byte, []store.RunLogInput) (int64, error)
	ListRunLogs(context.Context, string, string, int64, int) ([]store.RunLogEntry, int64, error)
}

type RunHistoryRepository interface {
	PurgeRunHistory(context.Context, string, string, time.Time, int) (int64, error)
}

type RunReportRepository interface {
	RunReport(context.Context, string, time.Time, time.Time, string) ([]store.RunReportPoint, error)
}

type OperationsService struct {
	logs    RunLogRepository
	history RunHistoryRepository
	reports RunReportRepository
}

type RunReportInput struct {
	TenantID, FromDate, ToDate, Timezone string
}

type RunReportResult struct {
	From, To time.Time
	Location *time.Location
	Points   []store.RunReportPoint
}

func NewOperationsService(logs RunLogRepository, history RunHistoryRepository, reports RunReportRepository) *OperationsService {
	return &OperationsService{logs: logs, history: history, reports: reports}
}

func (s *OperationsService) AppendRunLogs(ctx context.Context, runID, token string, entries []store.RunLogInput) (int64, error) {
	if runID == "" || token == "" {
		return 0, &ValidationError{err: fmt.Errorf("run_id and token are required")}
	}
	if err := validateRunLogInputs(entries); err != nil {
		return 0, &ValidationError{err: err}
	}
	hash := sha256.Sum256([]byte(token))
	return s.logs.AppendRunLogs(ctx, runID, hash[:], entries)
}

func (s *OperationsService) ListRunLogs(ctx context.Context, tenantID, runID string, after int64, limit int) ([]store.RunLogEntry, int64, error) {
	if tenantID == "" || runID == "" || after < 0 {
		return nil, 0, &ValidationError{err: fmt.Errorf("tenant_id, run_id and a non-negative cursor are required")}
	}
	return s.logs.ListRunLogs(ctx, tenantID, runID, after, limit)
}

func (s *OperationsService) PurgeRunHistory(ctx context.Context, tenantID, jobID string, before time.Time, limit int) (int64, error) {
	if tenantID == "" {
		return 0, &ValidationError{err: fmt.Errorf("tenant_id is required")}
	}
	if before.IsZero() {
		return 0, &ValidationError{err: fmt.Errorf("valid before timestamp is required")}
	}
	if jobID != "" && uuid.Validate(jobID) != nil {
		return 0, &ValidationError{err: fmt.Errorf("job_id must be a UUID")}
	}
	if limit < 0 || limit > 10000 {
		return 0, &ValidationError{err: fmt.Errorf("limit must be between 1 and 10000")}
	}
	if limit == 0 {
		limit = 1000
	}
	return s.history.PurgeRunHistory(ctx, tenantID, jobID, before, limit)
}

func (s *OperationsService) RunReport(ctx context.Context, input RunReportInput, now time.Time) (RunReportResult, error) {
	from, to, location, err := normalizeRunReportInput(input, now)
	if err != nil {
		return RunReportResult{}, &ValidationError{err: err}
	}
	points, err := s.reports.RunReport(ctx, input.TenantID, from, to, location.String())
	if err != nil {
		return RunReportResult{}, err
	}
	return RunReportResult{From: from, To: to, Location: location, Points: points}, nil
}

func validateRunLogInputs(entries []store.RunLogInput) error {
	if len(entries) == 0 || len(entries) > 100 {
		return fmt.Errorf("entries must contain between 1 and 100 items")
	}
	for _, entry := range entries {
		if strings.TrimSpace(entry.EntryID) == "" || len(entry.EntryID) > 128 {
			return fmt.Errorf("entry_id must contain between 1 and 128 characters")
		}
		if entry.Stream != "stdout" && entry.Stream != "stderr" {
			return fmt.Errorf("stream must be stdout or stderr")
		}
		if len(entry.Content) > maxRunLogEntryBytes {
			return fmt.Errorf("content must not exceed %d bytes", maxRunLogEntryBytes)
		}
	}
	return nil
}

func normalizeRunReportInput(input RunReportInput, now time.Time) (time.Time, time.Time, *time.Location, error) {
	if input.TenantID == "" {
		return time.Time{}, time.Time{}, nil, errors.New("tenant_id is required")
	}
	zoneName := input.Timezone
	if zoneName == "" {
		zoneName = "UTC"
	}
	location, err := time.LoadLocation(zoneName)
	if err != nil {
		return time.Time{}, time.Time{}, nil, errors.New("invalid timezone")
	}
	to := now.In(location)
	to = time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, location)
	from := to.AddDate(0, 0, -13)
	if input.FromDate != "" {
		from, err = time.ParseInLocation(time.DateOnly, input.FromDate, location)
		if err != nil {
			return time.Time{}, time.Time{}, nil, errors.New("from_date must use YYYY-MM-DD")
		}
	}
	if input.ToDate != "" {
		to, err = time.ParseInLocation(time.DateOnly, input.ToDate, location)
		if err != nil {
			return time.Time{}, time.Time{}, nil, errors.New("to_date must use YYYY-MM-DD")
		}
	}
	if to.Before(from) {
		return time.Time{}, time.Time{}, nil, errors.New("to_date must not precede from_date")
	}
	fromUTCDate := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	toUTCDate := time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC)
	if days := int(toUTCDate.Sub(fromUTCDate).Hours()/24) + 1; days > maxRunReportDays {
		return time.Time{}, time.Time{}, nil, errors.New("date range must not exceed 90 days")
	}
	return from, to, location, nil
}
