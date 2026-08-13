package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateNotificationConfig(kind string, raw json.RawMessage) error {
	switch kind {
	case "webhook":
		var config struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return fmt.Errorf("invalid webhook config")
		}
		parsed, err := url.ParseRequestURI(config.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return fmt.Errorf("webhook url must be an absolute HTTP or HTTPS URL without userinfo")
		}
	case "email":
		var config struct {
			To []string `json:"to"`
		}
		if err := json.Unmarshal(raw, &config); err != nil || len(config.To) == 0 {
			return fmt.Errorf("email config requires at least one recipient")
		}
		for _, recipient := range config.To {
			if !strings.Contains(recipient, "@") {
				return fmt.Errorf("invalid email recipient")
			}
		}
	default:
		return fmt.Errorf("kind must be webhook or email")
	}
	return nil
}

func (s *Service) CreateNotificationChannel(ctx context.Context, req *schedulerv1.CreateNotificationChannelRequest) (*schedulerv1.NotificationChannel, error) {
	if req.GetTenantId() == "" || strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and name are required")
	}
	if err := validateNotificationConfig(req.GetKind(), req.GetConfigJson()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	channel, err := s.store.CreateNotificationChannel(ctx, req.TenantId, req.Kind, strings.TrimSpace(req.Name), req.ConfigJson)
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
	return &schedulerv1.NotificationChannel{Id: channel.ID, TenantId: channel.TenantID, Kind: channel.Kind, Name: channel.Name, Configured: len(channel.Config) > 0}
}
