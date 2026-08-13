package rpc

import (
	"context"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type inProcessTestService struct {
	schedulerv1.UnimplementedSchedulerServiceServer
}

func (inProcessTestService) PreviewSchedule(context.Context, *schedulerv1.PreviewScheduleRequest) (*schedulerv1.PreviewScheduleResponse, error) {
	return &schedulerv1.PreviewScheduleResponse{}, nil
}

func TestInProcessSchedulerUsesAuthenticatedGRPC(t *testing.T) {
	scheduler, err := NewInProcessScheduler(inProcessTestService{}, "internal-token", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := scheduler.Close(ctx); closeErr != nil {
			t.Errorf("close: %v", closeErr)
		}
	})

	if _, err = scheduler.Client().PreviewSchedule(t.Context(), &schedulerv1.PreviewScheduleRequest{}); err != nil {
		t.Fatalf("in-process call failed: %v", err)
	}
}

func TestInProcessSchedulerRejectsCallsAfterClose(t *testing.T) {
	scheduler, err := NewInProcessScheduler(inProcessTestService{}, "server-token", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = scheduler.Close(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = scheduler.Client().PreviewSchedule(t.Context(), &schedulerv1.PreviewScheduleRequest{})
	if code := status.Code(err); code != codes.Canceled && code != codes.Unavailable {
		t.Fatalf("code = %s, want Canceled or Unavailable after shutdown", code)
	}
}
