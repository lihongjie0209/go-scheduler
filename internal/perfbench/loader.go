package perfbench

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

type ScheduledJob struct {
	Name        string
	EventID     string
	ScheduledAt time.Time
	SinkURL     string
}

type JobLoader interface {
	CreateScheduledJob(context.Context, ScheduledJob) (string, error)
}

func ValidateScheduledJob(job ScheduledJob) error {
	if job.Name == "" {
		return fmt.Errorf("job name is required")
	}
	if job.EventID == "" {
		return ErrEmptyEventID
	}
	if job.ScheduledAt.IsZero() {
		return ErrMissingScheduledAt
	}
	parsed, err := url.Parse(job.SinkURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("sink URL must be an absolute HTTP or HTTPS URL")
	}
	return nil
}

func QuartzCron(at time.Time) string {
	at = at.UTC()
	return fmt.Sprintf("%d %d %d %d %d ?", at.Second(), at.Minute(), at.Hour(), at.Day(), int(at.Month()))
}

func ExecutionURL(sinkURL, eventID string) (string, error) {
	parsed, err := url.Parse(sinkURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("id", eventID)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
