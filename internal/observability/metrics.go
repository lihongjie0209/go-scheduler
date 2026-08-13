package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequests  = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_http_requests_total", Help: "HTTP requests by method and outcome."}, []string{"method", "status_class"})
	HTTPDuration  = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "scheduler_http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"method"})
	Runs          = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_runs_total", Help: "Task executions by result."}, []string{"result"})
	RunDuration   = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "scheduler_run_duration_seconds", Help: "HTTP task execution latency.", Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30, 60, 300}})
	DispatchDelay = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scheduler_dispatch_delay_seconds",
		Help:    "Delay from a run's scheduled time until an engine worker starts processing it.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"trigger_type"})
	WorkerSaturationTicks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_worker_saturation_ticks_total",
		Help: "Scheduler ticks that could not claim work because every execution worker was occupied.",
	})
	// Delivery success rate:
	// sum by (provider, outcome) (rate(scheduler_notification_deliveries_total[5m]))
	NotificationDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scheduler_notification_deliveries_total",
		Help: "Notification delivery state transitions by provider and outcome.",
	}, []string{"provider", "outcome"})
	// P99 provider latency:
	// histogram_quantile(0.99, sum by (le, provider) (rate(scheduler_notification_delivery_duration_seconds_bucket[5m])))
	NotificationDeliveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scheduler_notification_delivery_duration_seconds",
		Help:    "Time spent calling notification providers.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"provider"})
)

func init() {
	prometheus.MustRegister(HTTPRequests, HTTPDuration, Runs, RunDuration, DispatchDelay, WorkerSaturationTicks, NotificationDeliveries, NotificationDeliveryDuration)
}
