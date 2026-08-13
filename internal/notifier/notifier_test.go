package notifier

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestRetryDelayIsBoundedExponential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Second}, {2, 4 * time.Second}, {8, 256 * time.Second}, {20, 300 * time.Second}}
	for _, tt := range tests {
		if got := retryDelay(tt.attempt, 2*time.Second, 300*time.Second); got != tt.want {
			t.Fatalf("retryDelay(%d)=%s want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestWebhookTemplateRendersLifecyclePayload(t *testing.T) {
	t.Parallel()
	event := store.OutboxEvent{ID: "event-1", Topic: "job.run.succeeded", Payload: []byte(`{"run_id":"run-1","status":"succeeded"}`)}
	body, err := webhookBody(`{"id":"{{.EventID}}","run":"{{.Payload.run_id}}","status":"{{.Payload.status}}"}`, event)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(body); got != `{"id":"event-1","run":"run-1","status":"succeeded"}` {
		t.Fatalf("rendered body = %s", got)
	}
}

func TestEmailContentRendersLifecyclePayload(t *testing.T) {
	t.Parallel()
	event := store.OutboxEvent{ID: "event-1", Topic: "job.run.failed", Payload: []byte(`{"job_id":"job-1","status":"failed"}`)}
	subject, body, contentType, err := emailContent(`Job {{.Payload.job_id}} {{.Payload.status}}`, `event={{.EventID}} topic={{.Topic}}`, event)
	if err != nil {
		t.Fatal(err)
	}
	if subject != "Job job-1 failed" || body != "event=event-1 topic=job.run.failed" || contentType != "text/plain; charset=utf-8" {
		t.Fatalf("email content = subject %q body %q type %q", subject, body, contentType)
	}
}

func TestEmailContentRejectsRenderedSubjectInjection(t *testing.T) {
	t.Parallel()
	event := store.OutboxEvent{Topic: "job.run.failed", Payload: []byte(`{"subject":"safe\r\nBcc: attacker@example.com"}`)}
	if _, _, _, err := emailContent(`{{.Payload.subject}}`, "", event); err == nil {
		t.Fatal("rendered email subject containing a newline was accepted")
	}
}

func TestEmailRequiresSTARTTLSByDefault(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		_, _ = connection.Write([]byte("220 smtp.example.test ESMTP\r\n"))
		reader := bufio.NewReader(connection)
		if _, readErr := reader.ReadString('\n'); readErr != nil {
			return
		}
		_, _ = connection.Write([]byte("250 smtp.example.test\r\n"))
		_, _ = reader.ReadString('\n')
	}()
	worker := New(nil, "test", SMTPConfig{Address: listener.Addr().String(), From: "scheduler@example.com"})
	channel := store.NotificationChannel{Config: json.RawMessage(`{"to":["ops@example.com"]}`)}
	event := store.OutboxEvent{Topic: "job.run.failed", Payload: []byte(`{"run_id":"run-1"}`)}
	if err = worker.email(t.Context(), channel, event); err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("email error = %v, want STARTTLS requirement", err)
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("fake SMTP server did not stop")
	}
}

func TestDingTalkHMACAuthenticationAndTemplate(t *testing.T) {
	t.Parallel()
	var gotToken, gotTimestamp, gotSignature, gotContent string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.URL.Query().Get("access_token")
		gotTimestamp = r.URL.Query().Get("timestamp")
		gotSignature = r.URL.Query().Get("sign")
		var body struct {
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotContent = body.Text.Content
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	t.Cleanup(target.Close)
	config, _ := json.Marshal(map[string]string{"url": target.URL, "auth_type": "hmac_sha256", "access_token": "robot-token", "secret": "robot-secret", "template": `run={{.Payload.run_id}} status={{.Payload.status}}`})
	worker := New(nil, "test", SMTPConfig{})
	event := store.OutboxEvent{ID: "event-1", Topic: "job.run.failed", Payload: []byte(`{"run_id":"run-1","status":"failed"}`)}
	if err := worker.dingtalk(t.Context(), store.NotificationChannel{Config: config}, event); err != nil {
		t.Fatal(err)
	}
	if gotToken != "robot-token" || gotTimestamp == "" || gotSignature == "" || gotContent != "run=run-1 status=failed" {
		t.Fatalf("dingtalk token=%q timestamp=%q signature=%q content=%q", gotToken, gotTimestamp, gotSignature, gotContent)
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

func TestNotificationHTTPErrorRedactsEndpointSecrets(t *testing.T) {
	t.Parallel()
	secret := "super-secret-token"
	worker := New(nil, "test", SMTPConfig{})
	worker.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("transport failure for " + req.URL.String())
	})}
	event := store.OutboxEvent{ID: "event-1", Topic: "job.run.failed", Payload: []byte(`{"run_id":"run-1"}`)}
	channels := []struct {
		name    string
		channel store.NotificationChannel
		send    func(context.Context, store.NotificationChannel, store.OutboxEvent) error
	}{
		{name: "webhook", channel: store.NotificationChannel{Config: json.RawMessage(`{"url":"https://example.test/hook?token=` + secret + `"}`)}, send: worker.webhook},
		{name: "dingtalk", channel: store.NotificationChannel{Config: json.RawMessage(`{"url":"https://example.test/robot","auth_type":"access_token","access_token":"` + secret + `"}`)}, send: worker.dingtalk},
	}
	for _, tt := range channels {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.send(t.Context(), tt.channel, event)
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatalf("error = %q, want redacted failure", err)
			}
		})
	}
}

func TestDeliverSafelyRecoversProviderPanicWithoutLeakingValue(t *testing.T) {
	t.Parallel()
	worker := New(nil, "test", SMTPConfig{})
	worker.senders["panic"] = func(context.Context, store.NotificationChannel, store.OutboxEvent) error {
		panic("credential=super-secret")
	}
	err := worker.deliverSafely(t.Context(), store.NotificationChannel{Kind: "panic", Name: "unsafe"}, store.OutboxEvent{})
	if err == nil || !strings.Contains(err.Error(), "provider panicked") || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("recovered provider error = %v", err)
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
	if got := completed.Load(); got != 30 {
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

func TestRunNotificationLoopImmediatelyDrainsFullBatches(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	drained := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		runNotificationLoop(ctx, time.Hour, 20, func() int {
			call := calls.Add(1)
			if call == 3 {
				close(drained)
				return 0
			}
			return 20
		})
	}()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("full notification batches were not drained immediately")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("drain calls = %d, want 3", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification loop did not stop after cancellation")
	}
}
