package perfbench

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type LoadRequest struct {
	RunID       string
	Count       int
	Concurrency int
	ScheduledAt time.Time
	SinkURL     string
}

type LoadedJob struct {
	JobID string        `json:"job_id"`
	Event ExpectedEvent `json:"event"`
}

func LoadScheduledJobs(ctx context.Context, loader JobLoader, request LoadRequest) ([]LoadedJob, error) {
	if request.RunID == "" || request.Count < 1 || request.Count > 100_000 || request.Concurrency < 1 || request.Concurrency > 256 {
		return nil, fmt.Errorf("run ID, count 1..100000, and concurrency 1..256 are required")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	results := make([]LoadedJob, request.Count)
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	for range request.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				eventID := fmt.Sprintf("%s-%06d", request.RunID, index)
				jobID, err := loader.CreateScheduledJob(ctx, ScheduledJob{Name: "scheduler-bench-" + eventID, EventID: eventID, ScheduledAt: request.ScheduledAt, SinkURL: request.SinkURL})
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("create benchmark job %d: %w", index, err)
						cancel()
					})
					continue
				}
				results[index] = LoadedJob{JobID: jobID, Event: ExpectedEvent{ID: eventID, ScheduledAt: request.ScheduledAt.UTC()}}
			}
		}()
	}
	for index := 0; index < request.Count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			if firstErr != nil {
				return nil, firstErr
			}
			return nil, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}
