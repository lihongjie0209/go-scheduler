package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestExecutorCommandQueueCollector(t *testing.T) {
	collector := NewExecutorCommandQueueCollector(func(context.Context) (ExecutorCommandQueueSnapshot, error) {
		return ExecutorCommandQueueSnapshot{Pending: 3, OldestPendingAge: 45 * time.Second}, nil
	})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	values := gatheredGaugeValues(t, registry)
	for name, want := range map[string]float64{
		"scheduler_executor_command_queue_collect_success":            1,
		"scheduler_executor_command_queue_oldest_pending_age_seconds": 45,
		"scheduler_executor_command_queue_pending":                    3,
	} {
		if got := values[name]; got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestExecutorCommandQueueCollectorReportsCollectionFailure(t *testing.T) {
	collector := NewExecutorCommandQueueCollector(func(context.Context) (ExecutorCommandQueueSnapshot, error) {
		return ExecutorCommandQueueSnapshot{}, errors.New("database unavailable")
	})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	values := gatheredGaugeValues(t, registry)
	if got := values["scheduler_executor_command_queue_collect_success"]; got != 0 {
		t.Fatalf("collection success = %v, want 0", got)
	}
	if _, exists := values["scheduler_executor_command_queue_pending"]; exists {
		t.Fatal("failed collection exported stale queue values")
	}
}
