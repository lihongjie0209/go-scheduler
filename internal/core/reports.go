package core

import (
	"context"
	"errors"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRunReportDays = 90

func normalizeRunReportRequest(req *schedulerv1.GetRunReportRequest, now time.Time) (time.Time, time.Time, *time.Location, error) {
	if req == nil || req.GetTenantId() == "" {
		return time.Time{}, time.Time{}, nil, errors.New("tenant_id is required")
	}
	zoneName := req.GetTimezone()
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
	if req.GetFromDate() != "" {
		from, err = time.ParseInLocation(time.DateOnly, req.GetFromDate(), location)
		if err != nil {
			return time.Time{}, time.Time{}, nil, errors.New("from_date must use YYYY-MM-DD")
		}
	}
	if req.GetToDate() != "" {
		to, err = time.ParseInLocation(time.DateOnly, req.GetToDate(), location)
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

func (s *Service) GetRunReport(ctx context.Context, req *schedulerv1.GetRunReportRequest) (*schedulerv1.RunReport, error) {
	from, to, location, err := normalizeRunReportRequest(req, time.Now())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	points, err := s.store.RunReport(ctx, req.GetTenantId(), from, to, location.String())
	if err != nil {
		return nil, toStatus(err)
	}
	response := &schedulerv1.RunReport{FromDate: from.Format(time.DateOnly), ToDate: to.Format(time.DateOnly), Timezone: location.String(), Points: make([]*schedulerv1.RunReportPoint, 0, len(points))}
	for _, point := range points {
		response.Points = append(response.Points, &schedulerv1.RunReportPoint{Date: point.Date, Total: point.Total, Succeeded: point.Succeeded, Failed: point.Failed, Active: point.Active, Cancelled: point.Cancelled, Skipped: point.Skipped})
	}
	return response, nil
}
