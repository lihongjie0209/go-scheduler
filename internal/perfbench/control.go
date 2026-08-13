package perfbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const expectationBatchSize = 10_000

func RegisterExpectations(ctx context.Context, client *http.Client, sinkURL string, events []ExpectedEvent) error {
	endpoint, err := sinkControlURL(sinkURL, "/api/v1/expect")
	if err != nil {
		return err
	}
	if client == nil {
		client = http.DefaultClient
	}
	for offset := 0; offset < len(events); offset += expectationBatchSize {
		end := min(offset+expectationBatchSize, len(events))
		payload, marshalErr := json.Marshal(map[string]any{"events": events[offset:end]})
		if marshalErr != nil {
			return marshalErr
		}
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if requestErr != nil {
			return requestErr
		}
		request.Header.Set("Content-Type", "application/json")
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return requestErr
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, maxRequestBody))
		closeErr := response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("benchmark sink expectation returned HTTP %d: %s", response.StatusCode, body)
		}
	}
	return nil
}

func BuildExpectedEvents(runID string, count int, scheduledAt time.Time) ([]ExpectedEvent, error) {
	if runID == "" || count < 1 || count > maxExpectedEvents || scheduledAt.IsZero() {
		return nil, fmt.Errorf("run ID, count 1..%d, and scheduled time are required", maxExpectedEvents)
	}
	events := make([]ExpectedEvent, count)
	for index := range events {
		events[index] = ExpectedEvent{ID: fmt.Sprintf("%s-%06d", runID, index), ScheduledAt: scheduledAt.UTC()}
	}
	return events, nil
}

func sinkControlURL(sinkURL, path string) (string, error) {
	parsed, err := url.Parse(sinkURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("sink URL must be an absolute HTTP or HTTPS URL")
	}
	parsed.Path = path
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
