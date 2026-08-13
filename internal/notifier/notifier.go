package notifier

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type SMTPConfig struct{ Address, Username, Password, From string }

const (
	notificationBatchSize   = 20
	notificationConcurrency = 10
	notificationTimeout     = 10 * time.Second
)

type Worker struct {
	store   *store.Store
	owner   string
	smtp    SMTPConfig
	client  *http.Client
	wg      sync.WaitGroup
	senders map[string]func(context.Context, store.NotificationChannel, store.OutboxEvent) error
}

func New(s *store.Store, owner string, smtpConfig SMTPConfig) *Worker {
	w := &Worker{store: s, owner: owner, smtp: smtpConfig, client: &http.Client{Timeout: notificationTimeout}}
	w.senders = map[string]func(context.Context, store.NotificationChannel, store.OutboxEvent) error{
		"webhook":  w.webhook,
		"email":    w.email,
		"dingtalk": w.dingtalk,
	}
	return w
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
	if err := w.store.PrepareNotificationDeliveries(ctx, notificationBatchSize); err != nil {
		slog.Error("prepare notification deliveries", "error", err)
		return
	}
	deliveries, err := w.store.ClaimNotificationDeliveries(ctx, w.owner, notificationBatchSize)
	if err != nil {
		slog.Error("claim notification deliveries", "error", err)
		return
	}
	processDeliveries(ctx, deliveries, notificationConcurrency, func(delivery store.NotificationDelivery) {
		deliveryCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
		deliverErr := w.deliver(deliveryCtx, delivery.Channel, delivery.Event)
		cancel()
		if deliverErr != nil {
			if delivery.Attempts >= delivery.Channel.MaxAttempts {
				if err := w.store.DeadLetterNotificationDelivery(ctx, delivery.ID, delivery.EventID, deliverErr.Error()); err != nil {
					slog.Error("dead-letter notification delivery", "delivery_id", delivery.ID, "error", err)
				}
				return
			}
			delay := retryDelay(delivery.Attempts, time.Duration(delivery.Channel.BackoffInitialSeconds)*time.Second, time.Duration(delivery.Channel.BackoffMaxSeconds)*time.Second)
			if err := w.store.RetryNotificationDelivery(ctx, delivery.ID, deliverErr.Error(), delay); err != nil {
				slog.Error("retry notification delivery", "delivery_id", delivery.ID, "error", err)
			}
			return
		}
		_ = w.store.CompleteNotificationDelivery(ctx, delivery.ID, delivery.EventID)
	})
}

func processDeliveries(ctx context.Context, deliveries []store.NotificationDelivery, concurrency int, process func(store.NotificationDelivery)) {
	if concurrency < 1 {
		concurrency = 1
	}
	semaphore := make(chan struct{}, concurrency)
	var group sync.WaitGroup
	for _, delivery := range deliveries {
		if ctx.Err() != nil {
			break
		}
		select {
		case semaphore <- struct{}{}:
			group.Add(1)
			go func() {
				defer group.Done()
				defer func() { <-semaphore }()
				process(delivery)
			}()
		case <-ctx.Done():
			group.Wait()
			return
		}
	}
	group.Wait()
}
func (w *Worker) deliver(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
	sender, ok := w.senders[channel.Kind]
	if !ok {
		return fmt.Errorf("unknown notification channel %q", channel.Kind)
	}
	err := sender(ctx, channel, event)
	if err != nil {
		return fmt.Errorf("deliver %s: %w", channel.Name, err)
	}
	return err
}
func (w *Worker) webhook(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
	var config struct {
		URL      string            `json:"url"`
		Headers  map[string]string `json:"headers"`
		Template string            `json:"template"`
	}
	if err := json.Unmarshal(channel.Config, &config); err != nil {
		return err
	}
	body, err := webhookBody(config.Template, event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, config.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for key, value := range config.Headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", event.ID)
	req.Header.Set("X-Go-Scheduler-Event-ID", event.ID)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", resp.StatusCode)
	}
	return nil
}

func (w *Worker) dingtalk(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
	var config struct {
		URL         string `json:"url"`
		AuthType    string `json:"auth_type"`
		AccessToken string `json:"access_token"`
		Secret      string `json:"secret"`
		Template    string `json:"template"`
	}
	if err := json.Unmarshal(channel.Config, &config); err != nil {
		return err
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil {
		return err
	}
	query := endpoint.Query()
	if config.AuthType == "access_token" || config.AccessToken != "" {
		query.Set("access_token", config.AccessToken)
	}
	if config.AuthType == "hmac_sha256" {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		mac := hmac.New(sha256.New, []byte(config.Secret))
		_, _ = mac.Write([]byte(timestamp + "\n" + config.Secret))
		query.Set("timestamp", timestamp)
		query.Set("sign", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	}
	endpoint.RawQuery = query.Encode()
	content, err := renderNotificationTemplate(config.Template, event)
	if err != nil {
		return err
	}
	if config.Template == "" {
		content = event.Topic + ": " + string(event.Payload)
	}
	body, err := json.Marshal(map[string]any{"msgtype": "text", "text": map[string]string{"content": content}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", event.ID)
	req.Header.Set("X-Go-Scheduler-Event-ID", event.ID)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dingtalk status %d", resp.StatusCode)
	}
	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if len(responseBody) > 0 && json.Unmarshal(responseBody, &result) == nil && result.ErrCode != 0 {
		return fmt.Errorf("dingtalk error %d: %s", result.ErrCode, result.ErrMsg)
	}
	return nil
}

func webhookBody(rawTemplate string, event store.OutboxEvent) ([]byte, error) {
	if rawTemplate == "" {
		return json.Marshal(map[string]any{"topic": event.Topic, "payload": json.RawMessage(event.Payload)})
	}
	rendered, err := renderNotificationTemplate(rawTemplate, event)
	if err != nil {
		return nil, err
	}
	if !json.Valid([]byte(rendered)) {
		return nil, fmt.Errorf("webhook template must render valid JSON")
	}
	return []byte(rendered), nil
}

func renderNotificationTemplate(rawTemplate string, event store.OutboxEvent) (string, error) {
	if len(rawTemplate) > 64<<10 {
		return "", fmt.Errorf("notification template exceeds 64 KiB")
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return "", fmt.Errorf("decode notification payload: %w", err)
	}
	tmpl, err := template.New("notification").Option("missingkey=error").Parse(rawTemplate)
	if err != nil {
		return "", err
	}
	var output bytes.Buffer
	if err = tmpl.Execute(&output, map[string]any{"EventID": event.ID, "Topic": event.Topic, "Payload": payload}); err != nil {
		return "", err
	}
	if output.Len() > 1<<20 {
		return "", fmt.Errorf("rendered notification exceeds 1 MiB")
	}
	return output.String(), nil
}
func (w *Worker) email(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) error {
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
	networkConnection, err := (&net.Dialer{Timeout: notificationTimeout}).DialContext(ctx, "tcp", w.smtp.Address)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err = networkConnection.SetDeadline(deadline); err != nil {
			_ = networkConnection.Close()
			return err
		}
	}
	conn, err := smtp.NewClient(networkConnection, host)
	if err != nil {
		_ = networkConnection.Close()
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
func retryDelay(attempt int, initial, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initial
	for step := 1; step < attempt && delay < maximum; step++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
