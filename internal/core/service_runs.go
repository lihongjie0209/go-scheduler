package core

import (
	"context"
	"fmt"
	"sort"
	"strings"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

func (s *Service) ListRuns(ctx context.Context, req *schedulerv1.ListRunsRequest) (*schedulerv1.ListRunsResponse, error) {
	runs, err := s.runs.List(ctx, req.TenantId, req.JobId, req.BroadcastGroupId, int(req.Limit))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListRunsResponse{Runs: make([]*schedulerv1.Run, 0, len(runs))}
	for _, r := range runs {
		out.Runs = append(out.Runs, runToProto(r))
	}
	return out, nil
}

func (s *Service) GetRun(ctx context.Context, req *schedulerv1.GetRunRequest) (*schedulerv1.Run, error) {
	run, err := s.runs.Get(ctx, req.GetTenantId(), req.GetRunId())
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(run), nil
}

func (s *Service) CancelRun(ctx context.Context, req *schedulerv1.CancelRunRequest) (*schedulerv1.Run, error) {
	run, err := s.runs.Cancel(ctx, req.GetTenantId(), req.GetRunId(), req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	return runToProto(run), nil
}

func normalizeDependencyIDs(ids []string) ([]string, error) {
	if len(ids) > 100 {
		return nil, fmt.Errorf("at most 100 child jobs are allowed")
	}
	unique := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("child job id must not be empty")
		}
		unique[id] = struct{}{}
	}
	out := make([]string, 0, len(unique))
	for id := range unique {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Service) SetJobDependencies(ctx context.Context, req *schedulerv1.SetJobDependenciesRequest) (*schedulerv1.JobDependencies, error) {
	ids, err := s.jobs.SetDependencies(ctx, req.GetTenantId(), req.GetParentJobId(), req.GetChildJobIds())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.JobDependencies{ParentJobId: req.GetParentJobId(), ChildJobIds: ids}, nil
}
func (s *Service) GetJobDependencies(ctx context.Context, req *schedulerv1.GetJobDependenciesRequest) (*schedulerv1.JobDependencies, error) {
	ids, err := s.jobs.GetDependencies(ctx, req.GetTenantId(), req.GetParentJobId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.JobDependencies{ParentJobId: req.GetParentJobId(), ChildJobIds: ids}, nil
}

func (s *Service) CompleteCallback(ctx context.Context, req *schedulerv1.CompleteCallbackRequest) (*schedulerv1.CompleteCallbackResponse, error) {
	if err := s.runs.CompleteCallback(ctx, req.GetRunId(), req.GetToken(), req.GetSucceeded(), req.GetMessage()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.CompleteCallbackResponse{}, nil
}
