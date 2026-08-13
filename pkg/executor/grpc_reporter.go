package executor

import (
	"context"
	"errors"

	"github.com/google/uuid"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

type GRPCReporter struct {
	client schedulerv1.SchedulerServiceClient
}

func NewGRPCReporter(client schedulerv1.SchedulerServiceClient) (*GRPCReporter, error) {
	if client == nil {
		return nil, errors.New("scheduler client is required")
	}
	return &GRPCReporter{client: client}, nil
}

func (r *GRPCReporter) AppendLog(ctx context.Context, runID, token, stream, content string) error {
	_, err := r.client.AppendRunLogs(ctx, &schedulerv1.AppendRunLogsRequest{RunId: runID, Token: token, Entries: []*schedulerv1.RunLogInput{{EntryId: uuid.NewString(), Stream: stream, Content: content}}})
	return err
}

func (r *GRPCReporter) Complete(ctx context.Context, runID, token string, succeeded bool, message string) error {
	_, err := r.client.CompleteCallback(ctx, &schedulerv1.CompleteCallbackRequest{RunId: runID, Token: token, Succeeded: succeeded, Message: message})
	return err
}
