package core

import (
	"testing"
	"time"
)

func TestDispatchDelay(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		startedAt time.Time
		scheduled time.Time
		want      time.Duration
	}{
		{name: "worker starts after schedule", startedAt: base.Add(275 * time.Millisecond), scheduled: base, want: 275 * time.Millisecond},
		{name: "same instant", startedAt: base, scheduled: base, want: 0},
		{name: "clock skew does not produce negative metric", startedAt: base, scheduled: base.Add(time.Millisecond), want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := dispatchDelay(test.startedAt, test.scheduled); got != test.want {
				t.Fatalf("dispatchDelay() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestAvailableWorkerSlots(t *testing.T) {
	t.Parallel()

	workers := make(chan struct{}, 3)
	if got := availableWorkerSlots(workers); got != 3 {
		t.Fatalf("empty worker pool has %d available slots, want 3", got)
	}
	workers <- struct{}{}
	workers <- struct{}{}
	if got := availableWorkerSlots(workers); got != 1 {
		t.Fatalf("partially occupied worker pool has %d available slots, want 1", got)
	}
	workers <- struct{}{}
	if got := availableWorkerSlots(workers); got != 0 {
		t.Fatalf("saturated worker pool has %d available slots, want 0", got)
	}
	<-workers
	if got := availableWorkerSlots(workers); got != 1 {
		t.Fatalf("released worker pool has %d available slots, want 1", got)
	}
}

func TestReleaseWorkerWakesDispatcher(t *testing.T) {
	t.Parallel()
	engine := &Engine{dispatchWake: make(chan struct{}, 1)}
	workers := make(chan struct{}, 1)
	workers <- struct{}{}

	engine.releaseWorker(workers)
	if len(workers) != 0 {
		t.Fatal("worker slot was not released")
	}
	select {
	case <-engine.dispatchWake:
	default:
		t.Fatal("dispatcher was not notified")
	}
}

func TestShouldWakeDispatcher(t *testing.T) {
	tests := []struct {
		name             string
		active, capacity int
		want             bool
	}{
		{name: "below refill threshold", active: 49, capacity: 64},
		{name: "at refill threshold", active: 48, capacity: 64, want: true},
		{name: "all workers idle", active: 0, capacity: 64, want: true},
		{name: "small pool frees one", active: 2, capacity: 3, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldWakeDispatcher(tt.active, tt.capacity); got != tt.want {
				t.Fatalf("shouldWakeDispatcher(%d, %d) = %v, want %v", tt.active, tt.capacity, got, tt.want)
			}
		})
	}
}
