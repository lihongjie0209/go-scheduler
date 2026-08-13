package perfbench

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCollectorReport(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	events := make([]ExpectedEvent, 5)
	for index := range events {
		events[index] = ExpectedEvent{ID: fmt.Sprintf("event-%d", index), ScheduledAt: base}
	}
	if err := collector.Expect(events); err != nil {
		t.Fatal(err)
	}
	for index, latency := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond} {
		duplicate, expected := collector.Observe(Observation{ID: events[index].ID, ReceivedAt: base.Add(latency)})
		if duplicate || !expected {
			t.Fatalf("first observation duplicate=%t expected=%t", duplicate, expected)
		}
	}
	duplicate, expected := collector.Observe(Observation{ID: events[0].ID, ReceivedAt: base.Add(time.Second)})
	if !duplicate || !expected {
		t.Fatalf("duplicate observation duplicate=%t expected=%t", duplicate, expected)
	}
	if duplicate, expected = collector.Observe(Observation{ID: "unexpected", ReceivedAt: base}); duplicate || expected {
		t.Fatalf("unexpected observation duplicate=%t expected=%t", duplicate, expected)
	}
	collector.Observe(Observation{})

	report := collector.Report()
	if report.Expected != 5 || report.Received != 4 || report.Missing != 1 {
		t.Fatalf("counts = %+v", report)
	}
	if report.DuplicateRequests != 1 || report.UnexpectedRequests != 1 || report.InvalidRequests != 1 {
		t.Fatalf("request counts = %+v", report)
	}
	if report.WindowSeconds != .04 || report.ThroughputPerSecond != 100 {
		t.Fatalf("window=%f throughput=%f", report.WindowSeconds, report.ThroughputPerSecond)
	}
	latency := report.LatencyMilliseconds
	if latency.Min != 10 || latency.P50 != 20 || latency.P90 != 40 || latency.P95 != 40 || latency.P99 != 40 || latency.P999 != 40 || latency.Max != 40 || latency.Mean != 25 {
		t.Fatalf("latency = %+v", latency)
	}
}

func TestCollectorExpectIsAtomic(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	err := collector.Expect([]ExpectedEvent{{ID: "valid", ScheduledAt: base}, {ID: "", ScheduledAt: base}})
	if !errors.Is(err, ErrEmptyEventID) {
		t.Fatalf("Expect() error = %v, want %v", err, ErrEmptyEventID)
	}
	if report := collector.Report(); report.Expected != 0 {
		t.Fatalf("partially committed batch: %+v", report)
	}
	if err = collector.Expect([]ExpectedEvent{{ID: "valid", ScheduledAt: base}}); err != nil {
		t.Fatal(err)
	}
	if err = collector.Expect([]ExpectedEvent{{ID: "valid", ScheduledAt: base.Add(time.Second)}}); err == nil {
		t.Fatal("conflicting scheduled time accepted")
	}
}

func TestCollectorConcurrentDuplicateDetection(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	if err := collector.Expect([]ExpectedEvent{{ID: "one", ScheduledAt: base}}); err != nil {
		t.Fatal(err)
	}
	const requests = 100
	var waitGroup sync.WaitGroup
	for range requests {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			collector.Observe(Observation{ID: "one", ReceivedAt: base.Add(time.Millisecond)})
		}()
	}
	waitGroup.Wait()
	report := collector.Report()
	if report.Received != 1 || report.DuplicateRequests != requests-1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCollectorReset(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	if err := collector.Expect([]ExpectedEvent{{ID: "one", ScheduledAt: base}}); err != nil {
		t.Fatal(err)
	}
	collector.Observe(Observation{ID: "one", ReceivedAt: base})
	collector.Observe(Observation{ID: "one", ReceivedAt: base})
	collector.MarkInvalid()
	collector.Reset()
	if report := collector.Report(); report != (Report{}) {
		t.Fatalf("report after reset = %+v", report)
	}
}
