package perfbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type GoSchedulerLoader struct {
	BaseURL  string
	Token    string
	TenantID string
	Client   *http.Client
}

func (l *GoSchedulerLoader) CreateScheduledJob(ctx context.Context, job ScheduledJob) (string, error) {
	if err := ValidateScheduledJob(job); err != nil {
		return "", err
	}
	targetURL, err := ExecutionURL(job.SinkURL, job.EventID)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(map[string]any{
		"name":                     job.Name,
		"schedule_type":            "cron",
		"schedule_expression":      QuartzCron(job.ScheduledAt),
		"timezone":                 "UTC",
		"target_url":               targetURL,
		"http_method":              http.MethodPost,
		"timeout_seconds":          10,
		"max_retries":              0,
		"overlap_policy":           "parallel",
		"misfire_policy":           "fire_once",
		"max_concurrent_runs":      1,
		"max_catch_up":             1,
		"callback_timeout_seconds": 30,
		"max_queue_size":           1,
		"enabled":                  true,
	})
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(l.BaseURL, "/")+"/api/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+l.Token)
	if l.TenantID != "" {
		request.Header.Set("X-Tenant-ID", l.TenantID)
	}
	response, err := l.httpClient().Do(request)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRequestBody))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("go scheduler create job returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var created struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(body, &created); err != nil {
		return "", fmt.Errorf("decode Go Scheduler create response: %w", err)
	}
	if created.ID == "" {
		return "", fmt.Errorf("go scheduler create response has no job ID")
	}
	return created.ID, nil
}

func (l *GoSchedulerLoader) httpClient() *http.Client {
	if l.Client != nil {
		return l.Client
	}
	return http.DefaultClient
}
