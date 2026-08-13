package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func TestDatabasePoolCollector(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewPedanticRegistry()
	collector := NewDatabasePoolCollector("api", func() store.PoolStats {
		return store.PoolStats{AcquiredConnections: 3, IdleConnections: 2, TotalConnections: 5, MaxConnections: 8, EmptyAcquireCount: 7, AcquireDuration: 250 * time.Millisecond}
	})
	if err := registry.Register(collector); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(NewDatabasePoolCollector("core", func() store.PoolStats {
		return store.PoolStats{MaxConnections: 24}
	})); err != nil {
		t.Fatalf("register second labeled pool collector: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.Metric {
			key := family.GetName()
			for _, label := range metric.Label {
				if label.GetName() == "pool" {
					key += ":" + label.GetValue()
				}
				if label.GetName() == "state" {
					key += ":" + label.GetValue()
				}
			}
			switch family.GetType().String() {
			case "GAUGE":
				values[key] = metric.GetGauge().GetValue()
			case "COUNTER":
				values[key] = metric.GetCounter().GetValue()
			}
		}
	}
	want := map[string]float64{
		"scheduler_database_pool_connections:api:acquired":           3,
		"scheduler_database_pool_connections:api:idle":               2,
		"scheduler_database_pool_connections:api:total":              5,
		"scheduler_database_pool_connections:api:max":                8,
		"scheduler_database_pool_connections:core:max":               24,
		"scheduler_database_pool_empty_acquires_total:api":           7,
		"scheduler_database_pool_acquire_duration_seconds_total:api": .25,
	}
	for key, expected := range want {
		if got := values[key]; got != expected {
			t.Errorf("%s = %v, want %v", key, got, expected)
		}
	}
}
