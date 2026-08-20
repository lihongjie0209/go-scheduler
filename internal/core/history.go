package core

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validatePurgeRunHistoryRequest(req *schedulerv1.PurgeRunHistoryRequest) error {
	if req == nil || req.GetTenantId() == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if req.GetBefore() == nil || !req.GetBefore().IsValid() {
		return fmt.Errorf("valid before timestamp is required")
	}
	if req.GetJobId() != "" {
		if _, err := uuid.Parse(req.GetJobId()); err != nil {
			return fmt.Errorf("job_id must be a UUID")
		}
	}
	if req.GetLimit() < 0 || req.GetLimit() > 10000 {
		return fmt.Errorf("limit must be between 1 and 10000")
	}
	return nil
}
func (s *Service) PurgeRunHistory(ctx context.Context, req *schedulerv1.PurgeRunHistoryRequest) (*schedulerv1.PurgeRunHistoryResponse, error) {
	if err := validatePurgeRunHistoryRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	deleted, err := s.operations.PurgeRunHistory(ctx, req.GetTenantId(), req.GetJobId(), req.GetBefore().AsTime(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.PurgeRunHistoryResponse{Deleted: deleted}, nil
}
