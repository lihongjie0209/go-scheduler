package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type ExecutorCommandQueueSnapshot struct {
	Pending          int64
	OldestPendingAge time.Duration
}

type ExecutorCommandQueueSnapshotFunc func(context.Context) (ExecutorCommandQueueSnapshot, error)

type ExecutorCommandQueueCollector struct {
	snapshot       ExecutorCommandQueueSnapshotFunc
	timeout        time.Duration
	pending        *prometheus.Desc
	oldestAge      *prometheus.Desc
	collectSuccess *prometheus.Desc
}

func NewExecutorCommandQueueCollector(snapshot ExecutorCommandQueueSnapshotFunc) *ExecutorCommandQueueCollector {
	return &ExecutorCommandQueueCollector{
		snapshot: snapshot,
		timeout:  2 * time.Second,
		pending: prometheus.NewDesc(
			"scheduler_executor_command_queue_pending",
			"Number of executor control commands waiting or leased for delivery.",
			nil,
			nil,
		),
		oldestAge: prometheus.NewDesc(
			"scheduler_executor_command_queue_oldest_pending_age_seconds",
			"Age of the oldest pending executor control command.",
			nil,
			nil,
		),
		collectSuccess: prometheus.NewDesc(
			"scheduler_executor_command_queue_collect_success",
			"Whether the most recent executor command queue database collection succeeded.",
			nil,
			nil,
		),
	}
}

func (c *ExecutorCommandQueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pending
	ch <- c.oldestAge
	ch <- c.collectSuccess
}

func (c *ExecutorCommandQueueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	snapshot, err := c.snapshot(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.collectSuccess, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(snapshot.Pending))
	ch <- prometheus.MustNewConstMetric(c.oldestAge, prometheus.GaugeValue, snapshot.OldestPendingAge.Seconds())
	ch <- prometheus.MustNewConstMetric(c.collectSuccess, prometheus.GaugeValue, 1)
}
