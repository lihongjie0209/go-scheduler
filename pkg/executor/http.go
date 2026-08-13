package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func HTTPHandler(client *http.Client) Handler {
	if client == nil {
		client = &http.Client{}
	}
	return func(ctx context.Context, task Task) error {
		if task.HTTP == nil || task.HTTP.URL == "" || task.HTTP.Method == "" {
			return errors.New("HTTP execution configuration is required")
		}
		request, err := http.NewRequestWithContext(ctx, task.HTTP.Method, task.HTTP.URL, bytes.NewBufferString(task.HTTP.Body))
		if err != nil {
			return err
		}
		for key, value := range task.HTTP.Headers {
			request.Header.Set(key, value)
		}
		executionID := task.ExternalExecutionID
		if executionID == "" {
			executionID = task.RunID
		}
		// Scheduler-owned identity headers deliberately override job headers so a
		// redelivered run cannot change its idempotency identity.
		request.Header.Set("Idempotency-Key", executionID)
		request.Header.Set("X-Go-Scheduler-Execution-ID", executionID)
		request.Header.Set("X-Go-Scheduler-Run-ID", task.RunID)
		request.Header.Set("X-Go-Scheduler-Job-ID", task.JobID)
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if readErr != nil {
			return readErr
		}
		if len(body) > 0 && task.Logger != nil {
			_ = task.Logger.Info(string(body))
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("HTTP target returned %d: %s", response.StatusCode, truncate(string(body), 4096))
		}
		return nil
	}
}
