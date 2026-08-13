package perfbench

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQuartzCron(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 13, 12, 34, 56, 0, time.FixedZone("offset", 8*60*60))
	if got := QuartzCron(at); got != "56 34 4 13 8 ?" {
		t.Fatalf("QuartzCron() = %q", got)
	}
}

func TestExecutionURLPreservesQuery(t *testing.T) {
	t.Parallel()
	got, err := ExecutionURL("https://sink.example/execute?source=bench", "run / 1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://sink.example/execute?id=run+%2F+1&source=bench" {
		t.Fatalf("ExecutionURL() = %q", got)
	}
}

type recordingLoader struct {
	mu     sync.Mutex
	jobs   []ScheduledJob
	failAt string
}

func (l *recordingLoader) CreateScheduledJob(_ context.Context, job ScheduledJob) (string, error) {
	if job.EventID == l.failAt {
		return "", errors.New("injected failure")
	}
	l.mu.Lock()
	l.jobs = append(l.jobs, job)
	l.mu.Unlock()
	return "job-" + job.EventID, nil
}

func TestLoadScheduledJobs(t *testing.T) {
	t.Parallel()
	loader := &recordingLoader{}
	at := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	loaded, err := LoadScheduledJobs(t.Context(), loader, LoadRequest{RunID: "burst", Count: 100, Concurrency: 8, ScheduledAt: at, SinkURL: "https://sink.example/execute"})
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 100 || len(loader.jobs) != 100 {
		t.Fatalf("loaded=%d calls=%d", len(loaded), len(loader.jobs))
	}
	for index, job := range loaded {
		wantEvent := fmt.Sprintf("burst-%06d", index)
		if job.JobID != "job-"+wantEvent || job.Event.ID != wantEvent || !job.Event.ScheduledAt.Equal(at) {
			t.Fatalf("loaded[%d] = %+v", index, job)
		}
	}
}

func TestLoadScheduledJobsCancelsOnFailure(t *testing.T) {
	t.Parallel()
	loader := &recordingLoader{failAt: "failure-000003"}
	_, err := LoadScheduledJobs(t.Context(), loader, LoadRequest{RunID: "failure", Count: 100, Concurrency: 1, ScheduledAt: time.Now(), SinkURL: "https://sink.example/execute"})
	if err == nil || !strings.Contains(err.Error(), "benchmark job 3") {
		t.Fatalf("LoadScheduledJobs() error = %v", err)
	}
}
