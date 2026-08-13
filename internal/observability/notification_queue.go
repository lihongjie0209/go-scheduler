package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type NotificationQueueSnapshot struct {
	Pending          int64
	OldestPendingAge time.Duration
}

type NotificationQueueSnapshotFunc func(context.Context) (NotificationQueueSnapshot, error)

type NotificationQueueCollector struct {
	snapshot       NotificationQueueSnapshotFunc
	timeout        time.Duration
	pending        *prometheus.Desc
	oldestAge      *prometheus.Desc
	collectSuccess *prometheus.Desc
}

func NewNotificationQueueCollector(snapshot NotificationQueueSnapshotFunc) *NotificationQueueCollector {
	return &NotificationQueueCollector{
		snapshot: snapshot,
		timeout:  2 * time.Second,
		// Backlog alert: scheduler_notification_queue_pending > 0
		pending: prometheus.NewDesc(
			"scheduler_notification_queue_pending",
			"Number of notification deliveries waiting or leased for delivery.",
			nil,
			nil,
		),
		// Stale backlog alert: scheduler_notification_queue_oldest_pending_age_seconds > 300
		oldestAge: prometheus.NewDesc(
			"scheduler_notification_queue_oldest_pending_age_seconds",
			"Age of the oldest pending notification delivery.",
			nil,
			nil,
		),
		// Collector health alert: scheduler_notification_queue_collect_success == 0
		collectSuccess: prometheus.NewDesc(
			"scheduler_notification_queue_collect_success",
			"Whether the most recent notification queue database collection succeeded.",
			nil,
			nil,
		),
	}
}

func (c *NotificationQueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pending
	ch <- c.oldestAge
	ch <- c.collectSuccess
}

func (c *NotificationQueueCollector) Collect(ch chan<- prometheus.Metric) {
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
