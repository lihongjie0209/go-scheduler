package notifier

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func TestRetryDelayIsBoundedExponential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Second}, {2, 4 * time.Second}, {8, 256 * time.Second}, {20, 256 * time.Second}}
	for _, tt := range tests {
		if got := retryDelay(tt.attempt); got != tt.want {
			t.Fatalf("retryDelay(%d)=%s want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestWebhookSendsIdempotencyHeaders(t *testing.T) {
	t.Parallel()
	var gotID, gotContentType string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = r.Header.Get("Idempotency-Key")
		gotContentType = r.Header.Get("Content-Type")
		_, _ = w.Write([]byte("accepted"))
	}))
	t.Cleanup(target.Close)
	config, err := json.Marshal(map[string]any{"url": target.URL, "headers": map[string]string{"Idempotency-Key": "attacker-controlled", "Content-Type": "text/plain"}})
	if err != nil {
		t.Fatal(err)
	}
	worker := New(nil, "test", SMTPConfig{})
	event := store.OutboxEvent{ID: "event-1", Topic: "job.run.failed", Payload: []byte(`{"run_id":"run-1"}`)}
	if err = worker.webhook(t.Context(), store.NotificationChannel{Config: config}, event); err != nil {
		t.Fatal(err)
	}
	if gotID != event.ID {
		t.Fatalf("Idempotency-Key = %q, want %q", gotID, event.ID)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q", gotContentType)
	}
}

func TestProcessDeliveriesBoundsConcurrency(t *testing.T) {
	t.Parallel()
	deliveries := make([]store.NotificationDelivery, 30)
	var active, peak, completed atomic.Int32
	processDeliveries(t.Context(), deliveries, 4, func(store.NotificationDelivery) {
		current := active.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		active.Add(-1)
		completed.Add(1)
	})
	if got := completed.Load(); got != int32(len(deliveries)) {
		t.Fatalf("completed = %d, want %d", got, len(deliveries))
	}
	if got := peak.Load(); got > 4 || got < 2 {
		t.Fatalf("peak concurrency = %d, want between 2 and 4", got)
	}
}

func TestProcessDeliveriesStopsSchedulingAfterCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var completed atomic.Int32
	processDeliveries(ctx, make([]store.NotificationDelivery, 10), 2, func(store.NotificationDelivery) {
		completed.Add(1)
	})
	if got := completed.Load(); got != 0 {
		t.Fatalf("completed after cancellation = %d", got)
	}
}
