package core

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func TestFixedExecutorForRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  store.Run
		job  store.Job
		want bool
	}{
		{name: "Docker retry remains on owning node", run: store.Run{TriggerType: "retry", ExecutorAddress: "executor-a:9090"}, job: store.Job{ScriptLanguage: "docker"}, want: true},
		{name: "reclaimed Docker run remains on owning node", run: store.Run{TriggerType: "manual", ExecutorAddress: "executor-a:9090"}, job: store.Job{ScriptLanguage: "docker"}, want: true},
		{name: "new Docker run may be routed", run: store.Run{}, job: store.Job{ScriptLanguage: "docker"}},
		{name: "Kubernetes recovery may move executors", run: store.Run{ExecutorAddress: "executor-a:9090"}, job: store.Job{ScriptLanguage: "kubernetes"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := fixedExecutorForRecovery(tt.run, tt.job); got != tt.want {
				t.Fatalf("fixedExecutorForRecovery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassifyRunFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "timed_out"},
		{name: "wrapped deadline", err: errors.New("request failed: " + context.DeadlineExceeded.Error()), want: "failed"},
		{name: "executor error", err: errors.New("connection refused"), want: "failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyRunFailure(tt.err); got != tt.want {
				t.Fatalf("classifyRunFailure() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestShouldRetry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		attempt    int32
		maxRetries int32
		want       bool
	}{
		{name: "no retries", attempt: 1, maxRetries: 0, want: false},
		{name: "first retry", attempt: 1, maxRetries: 1, want: true},
		{name: "retries exhausted", attempt: 2, maxRetries: 1, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldRetry(tt.attempt, tt.maxRetries); got != tt.want {
				t.Fatalf("shouldRetry(%d, %d) = %v, want %v", tt.attempt, tt.maxRetries, got, tt.want)
			}
		})
	}
}

func TestNewCallbackToken(t *testing.T) {
	t.Parallel()
	first, firstHash, err := newCallbackToken()
	if err != nil {
		t.Fatal(err)
	}
	second, secondHash, err := newCallbackToken()
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || len(firstHash) != 32 {
		t.Fatalf("invalid token or hash")
	}
	if first == second || bytes.Equal(firstHash, secondHash) {
		t.Fatal("tokens must be unique")
	}
}

func TestRetryDelayIsBoundedExponentialWithJitter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt  int32
		min, max time.Duration
	}{{1, time.Second, 1500 * time.Millisecond}, {2, 2 * time.Second, 3 * time.Second}, {7, time.Minute, 90 * time.Second}, {20, time.Minute, 90 * time.Second}}
	for _, tt := range tests {
		got := retryDelay(tt.attempt, ^uint16(0))
		if got < tt.min || got >= tt.max {
			t.Fatalf("attempt %d delay %s outside [%s,%s)", tt.attempt, got, tt.min, tt.max)
		}
	}
}

func TestExecutorCommandRetryDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{name: "normalizes zero", attempt: 0, want: time.Second},
		{name: "first attempt", attempt: 1, want: time.Second},
		{name: "exponential retry", attempt: 4, want: 8 * time.Second},
		{name: "bounded retry", attempt: 100, want: 5 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := executorCommandRetryDelay(tt.attempt); got != tt.want {
				t.Fatalf("executorCommandRetryDelay(%d) = %s, want %s", tt.attempt, got, tt.want)
			}
		})
	}
}
