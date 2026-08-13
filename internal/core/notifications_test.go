package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeNotificationEvents(t *testing.T) {
	t.Parallel()
	events, err := normalizeNotificationEvents([]string{"running", "job.run.succeeded", "running"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0] != "job.run.running" || events[1] != "job.run.succeeded" {
		t.Fatalf("events = %v", events)
	}
	if _, err = normalizeNotificationEvents([]string{"unknown"}); err == nil {
		t.Fatal("unknown event accepted")
	}
	if _, err = normalizeNotificationEvents(make([]string, 101)); err == nil {
		t.Fatal("oversized event list accepted")
	}
}

func TestListNotificationHistoryRejectsInvalidFilters(t *testing.T) {
	t.Parallel()
	service := &Service{}
	tests := []struct {
		name string
		req  *schedulerv1.ListNotificationHistoryRequest
	}{
		{name: "invalid channel", req: &schedulerv1.ListNotificationHistoryRequest{TenantId: "tenant", ChannelId: "not-a-uuid"}},
		{name: "invalid job", req: &schedulerv1.ListNotificationHistoryRequest{TenantId: "tenant", JobId: "not-a-uuid"}},
		{name: "invalid cursor", req: &schedulerv1.ListNotificationHistoryRequest{TenantId: "tenant", Cursor: "not-a-cursor"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := service.ListNotificationHistory(context.Background(), tt.req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

func TestNotificationHistoryCursorRoundTrip(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 14, 9, 30, 0, 123456000, time.UTC)
	id := "3f376a14-4005-42f0-bf88-bf743aa99ce7"
	raw := encodeNotificationHistoryCursor(createdAt, id)
	cursor, err := decodeNotificationHistoryCursor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.createdAt == nil || !cursor.createdAt.Equal(createdAt) || cursor.id == nil || *cursor.id != id {
		t.Fatalf("cursor = %+v", cursor)
	}
}

func TestNotificationConfigResourceLimits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind string
		raw  json.RawMessage
	}{
		{name: "oversized config", kind: "webhook", raw: json.RawMessage(strings.Repeat("x", maxNotificationConfigBytes+1))},
		{name: "oversized webhook template", kind: "webhook", raw: json.RawMessage(`{"url":"https://example.com","template":"` + strings.Repeat("x", maxNotificationTemplateBytes+1) + `"}`)},
		{name: "invalid webhook header", kind: "webhook", raw: json.RawMessage(`{"url":"https://example.com","headers":{"X-Test":"safe\r\nInjected: true"}}`)},
		{name: "too many email recipients", kind: "email", raw: mustJSON(t, map[string]any{"to": make([]string, maxNotificationRecipients+1)})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := validateNotificationConfig(tt.kind, tt.raw); err == nil {
				t.Fatal("resource limit was not enforced")
			}
		})
	}
}

func TestCreateNotificationChannelRejectsAmbiguousJobScope(t *testing.T) {
	t.Parallel()
	service := &Service{}
	_, err := service.CreateNotificationChannel(t.Context(), &schedulerv1.CreateNotificationChannelRequest{
		TenantId:   "tenant",
		Kind:       "webhook",
		Name:       "ambiguous",
		ConfigJson: []byte(`{"url":"https://example.com"}`),
		AllJobs:    true,
		JobIds:     []string{"3f376a14-4005-42f0-bf88-bf743aa99ce7"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, want InvalidArgument", status.Code(err))
	}
}

func TestNotificationChannelLifecycleRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	service := &Service{}
	if _, err := service.UpdateNotificationChannel(t.Context(), &schedulerv1.UpdateNotificationChannelRequest{TenantId: "tenant", Id: "bad", Kind: "webhook", Name: "name", ConfigJson: []byte(`{"url":"https://example.com"}`), AllJobs: true, Version: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("update status = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := service.SetNotificationChannelEnabled(t.Context(), &schedulerv1.SetNotificationChannelEnabledRequest{TenantId: "tenant", Id: "bad", Version: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("enable status = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := service.DeleteNotificationChannel(t.Context(), &schedulerv1.DeleteNotificationChannelRequest{TenantId: "tenant", Id: "bad", Version: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("delete status = %v, want InvalidArgument", status.Code(err))
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestValidateDingTalkConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{name: "hmac", config: `{"url":"https://oapi.dingtalk.com/robot/send","auth_type":"hmac_sha256","secret":"secret","template":"run={{.Payload.run_id}}"}`},
		{name: "missing secret", config: `{"url":"https://oapi.dingtalk.com/robot/send","auth_type":"hmac_sha256"}`, wantErr: true},
		{name: "plaintext URL", config: `{"url":"http://oapi.dingtalk.com/robot/send","auth_type":"none"}`, wantErr: true},
		{name: "invalid template", config: `{"url":"https://oapi.dingtalk.com/robot/send","template":"{{"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNotificationConfig("dingtalk", json.RawMessage(tt.config))
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate error = %v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
