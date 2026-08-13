package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"golang.org/x/net/http/httpguts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var notificationEventTypes = map[string]struct{}{
	"pending": {}, "running": {}, "waiting_callback": {}, "succeeded": {},
	"failed": {}, "timed_out": {}, "cancelled": {}, "skipped": {}, "exhausted": {},
}

const (
	maxNotificationConfigBytes   = 256 << 10
	maxNotificationTemplateBytes = 64 << 10
	maxNotificationRecipients    = 100
	maxNotificationHeaders       = 100
)

func validateNotificationConfig(kind string, raw json.RawMessage) error {
	if len(raw) == 0 || len(raw) > maxNotificationConfigBytes {
		return fmt.Errorf("notification config must be between 1 byte and 256 KiB")
	}
	switch kind {
	case "webhook":
		var config struct {
			URL      string            `json:"url"`
			Headers  map[string]string `json:"headers"`
			Template string            `json:"template"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return fmt.Errorf("invalid webhook config")
		}
		parsed, err := url.ParseRequestURI(config.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return fmt.Errorf("webhook url must be an absolute HTTP or HTTPS URL without userinfo")
		}
		if len(config.Headers) > maxNotificationHeaders {
			return fmt.Errorf("webhook config supports at most 100 headers")
		}
		for key, value := range config.Headers {
			if len(key) > 256 || len(value) > 8192 || !httpguts.ValidHeaderFieldName(key) || !httpguts.ValidHeaderFieldValue(value) {
				return fmt.Errorf("invalid webhook header size")
			}
		}
		if len(config.Template) > maxNotificationTemplateBytes {
			return fmt.Errorf("webhook template exceeds 64 KiB")
		}
		if config.Template != "" {
			if _, err = template.New("webhook").Option("missingkey=error").Parse(config.Template); err != nil {
				return fmt.Errorf("invalid webhook template")
			}
		}
	case "dingtalk":
		var config struct {
			URL         string `json:"url"`
			AuthType    string `json:"auth_type"`
			AccessToken string `json:"access_token"`
			Secret      string `json:"secret"`
			Template    string `json:"template"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return fmt.Errorf("invalid dingtalk config")
		}
		parsed, err := url.ParseRequestURI(config.URL)
		if err != nil || parsed.Host == "" || parsed.Scheme != "https" || parsed.User != nil {
			return fmt.Errorf("dingtalk url must be an absolute HTTPS URL without userinfo")
		}
		switch config.AuthType {
		case "", "none":
		case "access_token":
			if config.AccessToken == "" {
				return fmt.Errorf("dingtalk access_token authentication requires access_token")
			}
		case "hmac_sha256":
			if config.Secret == "" {
				return fmt.Errorf("dingtalk hmac_sha256 authentication requires secret")
			}
		default:
			return fmt.Errorf("dingtalk auth_type must be none, access_token, or hmac_sha256")
		}
		if len(config.Template) > maxNotificationTemplateBytes {
			return fmt.Errorf("dingtalk template exceeds 64 KiB")
		}
		if config.Template != "" {
			if _, err = template.New("dingtalk").Option("missingkey=error").Parse(config.Template); err != nil {
				return fmt.Errorf("invalid dingtalk template")
			}
		}
	case "email":
		var config struct {
			To []string `json:"to"`
		}
		if err := json.Unmarshal(raw, &config); err != nil || len(config.To) == 0 {
			return fmt.Errorf("email config requires at least one recipient")
		}
		if len(config.To) > maxNotificationRecipients {
			return fmt.Errorf("email config supports at most 100 recipients")
		}
		for _, recipient := range config.To {
			if !strings.Contains(recipient, "@") {
				return fmt.Errorf("invalid email recipient")
			}
		}
	default:
		return fmt.Errorf("kind must be webhook, email, or dingtalk")
	}
	return nil
}

func (s *Service) CreateNotificationChannel(ctx context.Context, req *schedulerv1.CreateNotificationChannelRequest) (*schedulerv1.NotificationChannel, error) {
	if req.GetTenantId() == "" || strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and name are required")
	}
	if len(strings.TrimSpace(req.GetName())) > 200 {
		return nil, status.Error(codes.InvalidArgument, "name must not exceed 200 bytes")
	}
	if err := validateNotificationConfig(req.GetKind(), req.GetConfigJson()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	events, err := normalizeNotificationEvents(req.GetEvents())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	jobIDs, err := normalizeNotificationJobIDs(req.GetJobIds())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetAllJobs() && len(jobIDs) > 0 {
		return nil, status.Error(codes.InvalidArgument, "all_jobs cannot be combined with job_ids")
	}
	allJobs := req.GetAllJobs() || len(jobIDs) == 0
	maxAttempts := int(req.GetMaxAttempts())
	if maxAttempts == 0 {
		maxAttempts = 8
	}
	initialBackoff := int(req.GetBackoffInitialSeconds())
	if initialBackoff == 0 {
		initialBackoff = 2
	}
	maxBackoff := int(req.GetBackoffMaxSeconds())
	if maxBackoff == 0 {
		maxBackoff = 300
	}
	if maxAttempts < 1 || maxAttempts > 100 || initialBackoff < 1 || initialBackoff > 3600 || maxBackoff < initialBackoff || maxBackoff > 86400 {
		return nil, status.Error(codes.InvalidArgument, "invalid retry policy")
	}
	channel, err := s.store.CreateNotificationChannel(ctx, store.NotificationChannel{TenantID: req.TenantId, Kind: req.Kind, Name: strings.TrimSpace(req.Name), Config: req.ConfigJson, Events: events, AllJobs: allJobs, JobIDs: jobIDs, MaxAttempts: maxAttempts, BackoffInitialSeconds: initialBackoff, BackoffMaxSeconds: maxBackoff})
	if err != nil {
		return nil, toStatus(err)
	}
	return notificationChannelToProto(channel), nil
}

func (s *Service) ListNotificationChannels(ctx context.Context, req *schedulerv1.ListNotificationChannelsRequest) (*schedulerv1.ListNotificationChannelsResponse, error) {
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	channels, err := s.store.NotificationChannels(ctx, req.TenantId)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListNotificationChannelsResponse{Channels: make([]*schedulerv1.NotificationChannel, 0, len(channels))}
	for _, channel := range channels {
		out.Channels = append(out.Channels, notificationChannelToProto(channel))
	}
	return out, nil
}

func notificationChannelToProto(channel store.NotificationChannel) *schedulerv1.NotificationChannel {
	return &schedulerv1.NotificationChannel{Id: channel.ID, TenantId: channel.TenantID, Kind: channel.Kind, Name: channel.Name, Configured: len(channel.Config) > 0, Events: trimEventPrefixes(channel.Events), AllJobs: channel.AllJobs, JobIds: channel.JobIDs, MaxAttempts: boundedInt32(channel.MaxAttempts), BackoffInitialSeconds: boundedInt32(channel.BackoffInitialSeconds), BackoffMaxSeconds: boundedInt32(channel.BackoffMaxSeconds)}
}

func boundedInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > 86400 {
		return 86400
	}
	return int32(value) // #nosec G115 -- bounded above to 86400.
}

func normalizeNotificationEvents(events []string) ([]string, error) {
	if len(events) > 100 {
		return nil, fmt.Errorf("at most 100 notification events are allowed")
	}
	if len(events) == 0 {
		events = []string{"exhausted"}
	}
	seen := make(map[string]struct{}, len(events))
	out := make([]string, 0, len(events))
	for _, event := range events {
		event = strings.TrimPrefix(strings.TrimSpace(event), "job.run.")
		if _, ok := notificationEventTypes[event]; !ok {
			return nil, fmt.Errorf("unsupported notification event %q", event)
		}
		if _, exists := seen[event]; exists {
			continue
		}
		seen[event] = struct{}{}
		out = append(out, "job.run."+event)
	}
	return out, nil
}

func trimEventPrefixes(events []string) []string {
	out := make([]string, len(events))
	for i, event := range events {
		out[i] = strings.TrimPrefix(event, "job.run.")
	}
	return out
}

func normalizeNotificationJobIDs(ids []string) ([]string, error) {
	if len(ids) > 10000 {
		return nil, fmt.Errorf("at most 10000 notification job IDs are allowed")
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if uuid.Validate(id) != nil {
			return nil, fmt.Errorf("invalid notification job id")
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func (s *Service) ListNotificationHistory(ctx context.Context, req *schedulerv1.ListNotificationHistoryRequest) (*schedulerv1.ListNotificationHistoryResponse, error) {
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetChannelId() != "" && uuid.Validate(req.GetChannelId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "channel_id must be a UUID")
	}
	if req.GetJobId() != "" && uuid.Validate(req.GetJobId()) != nil {
		return nil, status.Error(codes.InvalidArgument, "job_id must be a UUID")
	}
	if req.GetStatus() != "" && req.GetStatus() != "pending" && req.GetStatus() != "delivered" && req.GetStatus() != "dead" {
		return nil, status.Error(codes.InvalidArgument, "status must be pending, delivered, or dead")
	}
	limit := int(req.GetLimit())
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 {
		return nil, status.Error(codes.InvalidArgument, "limit must be between 1 and 500")
	}
	cursor, err := decodeNotificationHistoryCursor(req.GetCursor())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid notification history cursor")
	}
	entries, err := s.store.NotificationHistory(ctx, req.GetTenantId(), req.GetChannelId(), req.GetJobId(), req.GetStatus(), cursor.createdAt, cursor.id, limit+1)
	if err != nil {
		return nil, toStatus(err)
	}
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	out := &schedulerv1.ListNotificationHistoryResponse{Deliveries: make([]*schedulerv1.NotificationHistoryEntry, 0, len(entries))}
	for _, entry := range entries {
		item := &schedulerv1.NotificationHistoryEntry{DeliveryId: entry.DeliveryID, EventId: entry.EventID, ChannelId: entry.ChannelID, ChannelName: entry.ChannelName, ChannelKind: entry.ChannelKind, Topic: entry.Topic, JobId: entry.JobID, RunId: entry.RunID, Status: entry.Status, Attempts: boundedInt32(entry.Attempts), LastError: entry.LastError, CreatedAt: timestamppb.New(entry.CreatedAt)}
		if entry.DeliveredAt != nil {
			item.DeliveredAt = timestamppb.New(*entry.DeliveredAt)
		}
		if entry.DeadAt != nil {
			item.DeadAt = timestamppb.New(*entry.DeadAt)
		}
		out.Deliveries = append(out.Deliveries, item)
	}
	if hasMore {
		last := entries[len(entries)-1]
		out.NextCursor = encodeNotificationHistoryCursor(last.CreatedAt, last.DeliveryID)
	}
	return out, nil
}

type notificationHistoryCursor struct {
	createdAt *time.Time
	id        *string
}

type notificationHistoryCursorPayload struct {
	Version   int    `json:"v"`
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
}

func encodeNotificationHistoryCursor(createdAt time.Time, id string) string {
	payload, _ := json.Marshal(notificationHistoryCursorPayload{Version: 1, CreatedAt: createdAt.UTC().Format(time.RFC3339Nano), ID: id})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeNotificationHistoryCursor(raw string) (notificationHistoryCursor, error) {
	if raw == "" {
		return notificationHistoryCursor{}, nil
	}
	if len(raw) > 512 {
		return notificationHistoryCursor{}, fmt.Errorf("cursor is too long")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return notificationHistoryCursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	var payload notificationHistoryCursorPayload
	if err = json.Unmarshal(payloadBytes, &payload); err != nil || payload.Version != 1 || uuid.Validate(payload.ID) != nil {
		return notificationHistoryCursor{}, fmt.Errorf("decode cursor payload")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil || createdAt.IsZero() {
		return notificationHistoryCursor{}, fmt.Errorf("decode cursor timestamp")
	}
	return notificationHistoryCursor{createdAt: &createdAt, id: &payload.ID}, nil
}
