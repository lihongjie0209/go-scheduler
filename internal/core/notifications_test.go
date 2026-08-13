package core

import (
	"context"
	"encoding/json"
	"testing"

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
