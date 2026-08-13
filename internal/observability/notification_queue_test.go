package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNotificationQueueCollector(t *testing.T) {
	collector := NewNotificationQueueCollector(func(context.Context) (NotificationQueueSnapshot, error) {
		return NotificationQueueSnapshot{Pending: 7, OldestPendingAge: 12 * time.Second}, nil
	})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)

	values := gatheredGaugeValues(t, registry)
	for name, want := range map[string]float64{
		"scheduler_notification_queue_collect_success":            1,
		"scheduler_notification_queue_oldest_pending_age_seconds": 12,
		"scheduler_notification_queue_pending":                    7,
	} {
		if got := values[name]; got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestNotificationQueueCollectorReportsCollectionFailure(t *testing.T) {
	collector := NewNotificationQueueCollector(func(context.Context) (NotificationQueueSnapshot, error) {
		return NotificationQueueSnapshot{}, errors.New("database unavailable")
	})
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)

	values := gatheredGaugeValues(t, registry)
	if got := values["scheduler_notification_queue_collect_success"]; got != 0 {
		t.Fatalf("collection success = %v, want 0", got)
	}
	if _, exists := values["scheduler_notification_queue_pending"]; exists {
		t.Fatal("failed collection exported stale queue values")
	}
}

func gatheredGaugeValues(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]float64, len(families))
	for _, family := range families {
		if len(family.Metric) == 1 && family.Metric[0].Gauge != nil {
			values[family.GetName()] = family.Metric[0].GetGauge().GetValue()
		}
	}
	return values
}
