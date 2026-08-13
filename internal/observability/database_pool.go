package observability

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type DatabasePoolCollector struct {
	stats             func() store.PoolStats
	connections       *prometheus.Desc
	emptyAcquireCount *prometheus.Desc
	acquireDuration   *prometheus.Desc
}

func NewDatabasePoolCollector(pool string, stats func() store.PoolStats) *DatabasePoolCollector {
	labels := prometheus.Labels{"pool": pool}
	return &DatabasePoolCollector{
		stats:             stats,
		connections:       prometheus.NewDesc("scheduler_database_pool_connections", "PostgreSQL connections by pool and state.", []string{"state"}, labels),
		emptyAcquireCount: prometheus.NewDesc("scheduler_database_pool_empty_acquires_total", "Connection acquisitions that had to wait because no idle connection was available.", nil, labels),
		acquireDuration:   prometheus.NewDesc("scheduler_database_pool_acquire_duration_seconds_total", "Cumulative time spent acquiring PostgreSQL connections.", nil, labels),
	}
}

func (c *DatabasePoolCollector) Describe(channel chan<- *prometheus.Desc) {
	channel <- c.connections
	channel <- c.emptyAcquireCount
	channel <- c.acquireDuration
}

func (c *DatabasePoolCollector) Collect(channel chan<- prometheus.Metric) {
	stats := c.stats()
	connections := []struct {
		state string
		value int32
	}{
		{state: "acquired", value: stats.AcquiredConnections},
		{state: "idle", value: stats.IdleConnections},
		{state: "total", value: stats.TotalConnections},
		{state: "max", value: stats.MaxConnections},
	}
	for _, connection := range connections {
		channel <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(connection.value), connection.state)
	}
	channel <- prometheus.MustNewConstMetric(c.emptyAcquireCount, prometheus.CounterValue, float64(stats.EmptyAcquireCount))
	channel <- prometheus.MustNewConstMetric(c.acquireDuration, prometheus.CounterValue, stats.AcquireDuration.Seconds())
}
