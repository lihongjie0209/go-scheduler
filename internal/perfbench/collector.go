package perfbench

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

var (
	ErrEmptyEventID       = errors.New("event ID is required")
	ErrEventIDTooLong     = errors.New("event ID exceeds 256 bytes")
	ErrMissingScheduledAt = errors.New("scheduled time is required")
	ErrConflictingEvent   = errors.New("event already has a different scheduled time")
	ErrTooManyEvents      = errors.New("one expectation batch cannot exceed 100000 events")
)

const (
	maxEventIDLength  = 256
	maxExpectedEvents = 100_000
)

type ExpectedEvent struct {
	ID          string    `json:"id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type Observation struct {
	ID         string    `json:"id"`
	ReceivedAt time.Time `json:"received_at"`
}

type LatencySummary struct {
	Min  float64 `json:"min"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p99_9"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

type Report struct {
	Expected            int            `json:"expected"`
	Received            int            `json:"received"`
	Missing             int            `json:"missing"`
	DuplicateRequests   uint64         `json:"duplicate_requests"`
	UnexpectedRequests  uint64         `json:"unexpected_requests"`
	InvalidRequests     uint64         `json:"invalid_requests"`
	WindowSeconds       float64        `json:"window_seconds"`
	ThroughputPerSecond float64        `json:"throughput_per_second"`
	LatencyMilliseconds LatencySummary `json:"latency_ms"`
}

type observedEvent struct {
	scheduledAt time.Time
	receivedAt  time.Time
}

type Collector struct {
	mu         sync.RWMutex
	expected   map[string]time.Time
	received   map[string]observedEvent
	duplicates uint64
	unexpected uint64
	invalid    uint64
}

func NewCollector() *Collector {
	return &Collector{expected: make(map[string]time.Time), received: make(map[string]observedEvent)}
}

func (c *Collector) Expect(events []ExpectedEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(events) > maxExpectedEvents {
		return ErrTooManyEvents
	}
	batch := make(map[string]time.Time, len(events))
	for _, event := range events {
		if event.ID == "" {
			return ErrEmptyEventID
		}
		if len(event.ID) > maxEventIDLength {
			return fmt.Errorf("event %q: %w", event.ID[:maxEventIDLength], ErrEventIDTooLong)
		}
		if event.ScheduledAt.IsZero() {
			return fmt.Errorf("event %q: %w", event.ID, ErrMissingScheduledAt)
		}
		scheduledAt := event.ScheduledAt.UTC()
		if existing, ok := batch[event.ID]; ok && !existing.Equal(scheduledAt) {
			return fmt.Errorf("event %q: %w", event.ID, ErrConflictingEvent)
		}
		if existing, ok := c.expected[event.ID]; ok && !existing.Equal(scheduledAt) {
			return fmt.Errorf("event %q: %w", event.ID, ErrConflictingEvent)
		}
		batch[event.ID] = scheduledAt
	}
	for id, scheduledAt := range batch {
		c.expected[id] = scheduledAt
	}
	return nil
}

func (c *Collector) Observe(observation Observation) (duplicate bool, expected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if observation.ID == "" || len(observation.ID) > maxEventIDLength || observation.ReceivedAt.IsZero() {
		c.invalid++
		return false, false
	}
	scheduledAt, ok := c.expected[observation.ID]
	if !ok {
		c.unexpected++
		return false, false
	}
	if _, ok = c.received[observation.ID]; ok {
		c.duplicates++
		return true, true
	}
	c.received[observation.ID] = observedEvent{scheduledAt: scheduledAt, receivedAt: observation.ReceivedAt.UTC()}
	return false, true
}

func (c *Collector) MarkInvalid() {
	c.mu.Lock()
	c.invalid++
	c.mu.Unlock()
}

func (c *Collector) Reset() {
	c.mu.Lock()
	c.expected = make(map[string]time.Time)
	c.received = make(map[string]observedEvent)
	c.duplicates = 0
	c.unexpected = 0
	c.invalid = 0
	c.mu.Unlock()
}

func (c *Collector) Report() Report {
	c.mu.RLock()
	defer c.mu.RUnlock()

	report := Report{
		Expected:           len(c.expected),
		Received:           len(c.received),
		Missing:            len(c.expected) - len(c.received),
		DuplicateRequests:  c.duplicates,
		UnexpectedRequests: c.unexpected,
		InvalidRequests:    c.invalid,
	}
	latencies := make([]float64, 0, len(c.received))
	var earliestScheduledAt, latestReceivedAt time.Time
	for _, event := range c.received {
		latency := event.receivedAt.Sub(event.scheduledAt).Seconds() * 1000
		latencies = append(latencies, latency)
		if earliestScheduledAt.IsZero() || event.scheduledAt.Before(earliestScheduledAt) {
			earliestScheduledAt = event.scheduledAt
		}
		if event.receivedAt.After(latestReceivedAt) {
			latestReceivedAt = event.receivedAt
		}
	}
	if !earliestScheduledAt.IsZero() && latestReceivedAt.After(earliestScheduledAt) {
		report.WindowSeconds = latestReceivedAt.Sub(earliestScheduledAt).Seconds()
		report.ThroughputPerSecond = float64(report.Received) / report.WindowSeconds
	}
	report.LatencyMilliseconds = summarizeLatencies(latencies)
	return report
}

func summarizeLatencies(values []float64) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	sort.Float64s(values)
	var total float64
	for _, value := range values {
		total += value
	}
	return LatencySummary{
		Min:  values[0],
		P50:  nearestRank(values, .50),
		P90:  nearestRank(values, .90),
		P95:  nearestRank(values, .95),
		P99:  nearestRank(values, .99),
		P999: nearestRank(values, .999),
		Max:  values[len(values)-1],
		Mean: total / float64(len(values)),
	}
}

func nearestRank(sorted []float64, percentile float64) float64 {
	rank := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
