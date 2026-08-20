package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type NotificationChannelReader interface {
	NotificationChannel(context.Context, string, string) (store.NotificationChannel, error)
	NotificationChannels(context.Context, string) ([]store.NotificationChannel, error)
}

type NotificationChannelWriter interface {
	CreateNotificationChannel(context.Context, store.NotificationChannel) (store.NotificationChannel, error)
	UpdateNotificationChannel(context.Context, store.NotificationChannel) (store.NotificationChannel, error)
	SetNotificationChannelEnabled(context.Context, string, string, bool, int64) (store.NotificationChannel, error)
	DeleteNotificationChannel(context.Context, string, string, int64) error
}

type NotificationHistoryReader interface {
	NotificationHistory(context.Context, string, string, string, string, *time.Time, *string, int) ([]store.NotificationHistoryEntry, error)
}

type NotificationService struct {
	reader  NotificationChannelReader
	writer  NotificationChannelWriter
	history NotificationHistoryReader
}

type NotificationChannelInput struct {
	TenantID, ID, Kind, Name                string
	Config                                  json.RawMessage
	Events, JobIDs                          []string
	AllJobs                                 bool
	MaxAttempts, InitialBackoff, MaxBackoff int32
	Version                                 int64
}

type NotificationHistoryInput struct {
	TenantID, ChannelID, JobID, Status, Cursor string
	Limit                                      int
}

type NotificationHistoryResult struct {
	Entries    []store.NotificationHistoryEntry
	NextCursor string
}

func NewNotificationService(reader NotificationChannelReader, writer NotificationChannelWriter, history NotificationHistoryReader) *NotificationService {
	return &NotificationService{reader: reader, writer: writer, history: history}
}

func (s *NotificationService) Create(ctx context.Context, input NotificationChannelInput) (store.NotificationChannel, error) {
	channel, err := notificationChannelFromInput(input)
	if err != nil {
		return store.NotificationChannel{}, &ValidationError{err: err}
	}
	return s.writer.CreateNotificationChannel(ctx, channel)
}

func (s *NotificationService) Update(ctx context.Context, input NotificationChannelInput) (store.NotificationChannel, error) {
	if input.TenantID == "" || input.Version < 1 || uuid.Validate(input.ID) != nil {
		return store.NotificationChannel{}, &ValidationError{err: fmt.Errorf("UUID id and positive version are required")}
	}
	if len(input.Config) == 0 {
		current, err := s.reader.NotificationChannel(ctx, input.TenantID, input.ID)
		if err != nil {
			return store.NotificationChannel{}, err
		}
		if current.Kind != input.Kind {
			return store.NotificationChannel{}, &ValidationError{err: fmt.Errorf("config_json is required when changing notification channel kind")}
		}
		input.Config = current.Config
	}
	channel, err := notificationChannelFromInput(input)
	if err != nil {
		return store.NotificationChannel{}, &ValidationError{err: err}
	}
	return s.writer.UpdateNotificationChannel(ctx, channel)
}

func (s *NotificationService) SetEnabled(ctx context.Context, tenantID, id string, enabled bool, version int64) (store.NotificationChannel, error) {
	if tenantID == "" || uuid.Validate(id) != nil || version < 1 {
		return store.NotificationChannel{}, &ValidationError{err: fmt.Errorf("tenant_id, UUID id and positive version are required")}
	}
	return s.writer.SetNotificationChannelEnabled(ctx, tenantID, id, enabled, version)
}

func (s *NotificationService) Delete(ctx context.Context, tenantID, id string, version int64) error {
	if tenantID == "" || uuid.Validate(id) != nil || version < 1 {
		return &ValidationError{err: fmt.Errorf("tenant_id, UUID id and positive version are required")}
	}
	return s.writer.DeleteNotificationChannel(ctx, tenantID, id, version)
}

func (s *NotificationService) List(ctx context.Context, tenantID string) ([]store.NotificationChannel, error) {
	if tenantID == "" {
		return nil, &ValidationError{err: fmt.Errorf("tenant_id is required")}
	}
	return s.reader.NotificationChannels(ctx, tenantID)
}

func (s *NotificationService) ListHistory(ctx context.Context, input NotificationHistoryInput) (NotificationHistoryResult, error) {
	if input.TenantID == "" {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("tenant_id is required")}
	}
	if input.ChannelID != "" && uuid.Validate(input.ChannelID) != nil {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("channel_id must be a UUID")}
	}
	if input.JobID != "" && uuid.Validate(input.JobID) != nil {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("job_id must be a UUID")}
	}
	if input.Status != "" && input.Status != "pending" && input.Status != "delivered" && input.Status != "dead" {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("status must be pending, delivered, or dead")}
	}
	if input.Limit == 0 {
		input.Limit = 100
	}
	if input.Limit < 1 || input.Limit > 500 {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("limit must be between 1 and 500")}
	}
	cursor, err := decodeNotificationHistoryCursor(input.Cursor)
	if err != nil {
		return NotificationHistoryResult{}, &ValidationError{err: fmt.Errorf("invalid notification history cursor")}
	}
	entries, err := s.history.NotificationHistory(ctx, input.TenantID, input.ChannelID, input.JobID, input.Status, cursor.createdAt, cursor.id, input.Limit+1)
	if err != nil {
		return NotificationHistoryResult{}, err
	}
	result := NotificationHistoryResult{Entries: entries}
	if len(entries) > input.Limit {
		result.Entries = entries[:input.Limit]
		last := result.Entries[len(result.Entries)-1]
		result.NextCursor = encodeNotificationHistoryCursor(last.CreatedAt, last.DeliveryID)
	}
	return result, nil
}

func notificationChannelFromInput(input NotificationChannelInput) (store.NotificationChannel, error) {
	return notificationChannelForWrite(input.TenantID, input.ID, input.Kind, input.Name, input.Config, input.Events, input.AllJobs, input.JobIDs, input.MaxAttempts, input.InitialBackoff, input.MaxBackoff, input.Version)
}
