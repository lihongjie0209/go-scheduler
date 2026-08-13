package notifier

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type SMTPConfig struct{ Address, Username, Password, From string }
type Worker struct {
	store  *store.Store
	owner  string
	smtp   SMTPConfig
	client *http.Client
	wg     sync.WaitGroup
}

func New(s *store.Store, owner string, smtpConfig SMTPConfig) *Worker {
	return &Worker{store: s, owner: owner, smtp: smtpConfig, client: &http.Client{Timeout: 10 * time.Second}}
}
func (w *Worker) Run(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			w.tick(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
func (w *Worker) Wait() { w.wg.Wait() }
func (w *Worker) tick(ctx context.Context) {
	if err := w.store.PrepareNotificationDeliveries(ctx, 50); err != nil {
		slog.Error("prepare notification deliveries", "error", err)
		return
	}
	deliveries, err := w.store.ClaimNotificationDeliveries(ctx, w.owner, 50)
	if err != nil {
		slog.Error("claim notification deliveries", "error", err)
		return
	}
	for _, delivery := range deliveries {
		if err = w.deliver(ctx, delivery.Channel, delivery.Event); err != nil {
			_ = w.store.RetryNotificationDelivery(ctx, delivery.ID, err.Error(), retryDelay(delivery.Attempts))
			continue
		}
		_ = w.store.CompleteNotificationDelivery(ctx, delivery.ID, delivery.EventID)
	}
}
func (w *Worker) deliver(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
	var err error
	switch channel.Kind {
	case "webhook":
		err = w.webhook(ctx, channel, event)
	case "email":
		err = w.email(channel, event)
	default:
		err = fmt.Errorf("unknown notification channel %q", channel.Kind)
	}
	if err != nil {
		return fmt.Errorf("deliver %s: %w", channel.Name, err)
	}
	return err
}
func (w *Worker) webhook(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
	var config struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(channel.Config, &config); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"topic": event.Topic, "payload": json.RawMessage(event.Payload)})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range config.Headers {
		req.Header.Set(key, value)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", resp.StatusCode)
	}
	return nil
}
func (w *Worker) email(channel store.NotificationChannel, event store.OutboxEvent) error {
	if w.smtp.Address == "" {
		return fmt.Errorf("SMTP is not configured")
	}
	var config struct {
		To []string `json:"to"`
	}
	if err := json.Unmarshal(channel.Config, &config); err != nil {
		return err
	}
	host, _, found := strings.Cut(w.smtp.Address, ":")
	if !found {
		return fmt.Errorf("invalid SMTP address")
	}
	conn, err := smtp.Dial(w.smtp.Address)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if ok, _ := conn.Extension("STARTTLS"); ok {
		if err = conn.StartTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}); err != nil {
			return err
		}
	}
	if w.smtp.Username != "" {
		if err = conn.Auth(smtp.PlainAuth("", w.smtp.Username, w.smtp.Password, host)); err != nil {
			return err
		}
	}
	if err = conn.Mail(w.smtp.From); err != nil {
		return err
	}
	for _, to := range config.To {
		if err = conn.Rcpt(to); err != nil {
			return err
		}
	}
	writer, err := conn.Data()
	if err != nil {
		return err
	}
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: Scheduler event %s\r\nContent-Type: application/json\r\n\r\n%s", w.smtp.From, strings.Join(config.To, ","), event.Topic, event.Payload)
	if _, err = writer.Write([]byte(message)); err != nil {
		return err
	}
	return writer.Close()
}
func retryDelay(attempt int) time.Duration {
	if attempt > 8 {
		attempt = 8
	}
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(1<<attempt) * time.Second
}
