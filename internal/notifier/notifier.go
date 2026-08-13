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
	"net/mail"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type SMTPConfig struct{ Address, Username, Password, From, TLSMode string }

const (
	notificationBatchSize   = 20
	notificationConcurrency = 10
	notificationTimeout     = 10 * time.Second
	notificationIdlePoll    = 2 * time.Second
	maxProviderResponseSize = 64 << 10
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
		runNotificationLoop(ctx, notificationIdlePoll, notificationBatchSize, func() int { return w.tick(ctx) })
	}()
}
func (w *Worker) Wait() { w.wg.Wait() }

func runNotificationLoop(ctx context.Context, idlePoll time.Duration, batchSize int, drain func() int) {
	if idlePoll <= 0 {
		idlePoll = notificationIdlePoll
	}
	if batchSize < 1 {
		batchSize = 1
	}
	timer := time.NewTimer(idlePoll)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		processed := drain()
		if ctx.Err() != nil {
			return
		}
		if processed >= batchSize {
			continue
		}
		timer.Reset(idlePoll)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) tick(ctx context.Context) int {
	if err := w.store.PrepareNotificationDeliveries(ctx, notificationBatchSize); err != nil {
		slog.Error("prepare notification deliveries", "error", err)
		return 0
	}
	deliveries, err := w.store.ClaimNotificationDeliveries(ctx, w.owner, notificationBatchSize)
	if err != nil {
		slog.Error("claim notification deliveries", "error", err)
		return 0
	}
	processDeliveries(ctx, deliveries, notificationConcurrency, func(delivery store.NotificationDelivery) {
		provider := notificationProvider(delivery.Channel.Kind)
		deliverErr := w.attemptDelivery(ctx, delivery, provider)
		if deliverErr != nil {
			if delivery.Attempts >= delivery.Channel.MaxAttempts {
				if err := w.store.DeadLetterNotificationDelivery(ctx, w.owner, delivery.ID, delivery.EventID, deliverErr.Error()); err != nil {
					slog.Error("dead-letter notification delivery", "delivery_id", delivery.ID, "error", err)
					observability.NotificationDeliveries.WithLabelValues(provider, "state_error").Inc()
				} else {
					observability.NotificationDeliveries.WithLabelValues(provider, "dead").Inc()
				}
				return
			}
			delay := retryDelay(delivery.Attempts, time.Duration(delivery.Channel.BackoffInitialSeconds)*time.Second, time.Duration(delivery.Channel.BackoffMaxSeconds)*time.Second)
			if err := w.store.RetryNotificationDelivery(ctx, w.owner, delivery.ID, deliverErr.Error(), delay); err != nil {
				slog.Error("retry notification delivery", "delivery_id", delivery.ID, "error", err)
				observability.NotificationDeliveries.WithLabelValues(provider, "state_error").Inc()
			} else {
				observability.NotificationDeliveries.WithLabelValues(provider, "retry").Inc()
			}
			return
		}
		if err := w.store.CompleteNotificationDelivery(ctx, w.owner, delivery.ID, delivery.EventID); err != nil {
			slog.Error("complete notification delivery", "delivery_id", delivery.ID, "error", err)
			observability.NotificationDeliveries.WithLabelValues(provider, "state_error").Inc()
		} else {
			observability.NotificationDeliveries.WithLabelValues(provider, "delivered").Inc()
		}
	})
	return len(deliveries)
}

func (w *Worker) attemptDelivery(ctx context.Context, delivery store.NotificationDelivery, provider string) error {
	if delivery.LoadError != nil {
		return delivery.LoadError
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, notificationTimeout)
	defer cancel()
	startedAt := time.Now()
	err := w.deliverSafely(deliveryCtx, delivery.Channel, delivery.Event)
	observability.NotificationDeliveryDuration.WithLabelValues(provider).Observe(time.Since(startedAt).Seconds())
	return err
}

func notificationProvider(kind string) string {
	switch kind {
	case "webhook", "email", "dingtalk":
		return kind
	default:
		return "unknown"
	}
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

func (w *Worker) deliverSafely(ctx context.Context, channel store.NotificationChannel, event store.OutboxEvent) (err error) {
	defer func() {
		if recover() != nil {
			// Provider implementations may handle secret-bearing configuration. Do not
			// persist or log the recovered value because it may contain credentials.
			err = fmt.Errorf("notification provider panicked")
		}
	}()
	return w.deliver(ctx, channel, event)
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
		return redactedHTTPRequestError("webhook", err)
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
		return redactedHTTPRequestError("dingtalk", err)
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxProviderResponseSize+1))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dingtalk status %d", resp.StatusCode)
	}
	if readErr != nil {
		return fmt.Errorf("dingtalk response read failed")
	}
	if len(responseBody) > maxProviderResponseSize {
		return fmt.Errorf("dingtalk response exceeds 64 KiB")
	}
	return validateDingTalkResponse(responseBody)
}

func validateDingTalkResponse(responseBody []byte) error {
	var result struct {
		ErrCode *int `json:"errcode"`
	}
	if len(responseBody) == 0 || json.Unmarshal(responseBody, &result) != nil || result.ErrCode == nil {
		return fmt.Errorf("dingtalk returned an invalid response")
	}
	if *result.ErrCode != 0 {
		// Do not persist the remote errmsg. Notification endpoints are configurable
		// and an untrusted endpoint could reflect credentials into delivery history.
		return fmt.Errorf("dingtalk error %d", *result.ErrCode)
	}
	return nil
}

func redactedHTTPRequestError(provider string, err error) error {
	if err == nil {
		return nil
	}
	// net/http wraps transport failures in url.Error, whose Error method includes
	// the complete request URL. Notification endpoints commonly carry tokens in
	// the query string, so persisted retry errors must not wrap or format it.
	return fmt.Errorf("%s request failed", provider)
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
		To       []string `json:"to"`
		Subject  string   `json:"subject"`
		Template string   `json:"template"`
	}
	if err := json.Unmarshal(channel.Config, &config); err != nil {
		return err
	}
	from, err := mail.ParseAddress(w.smtp.From)
	if err != nil {
		return fmt.Errorf("invalid SMTP from address")
	}
	recipients := make([]*mail.Address, 0, len(config.To))
	for _, raw := range config.To {
		recipient, parseErr := mail.ParseAddress(raw)
		if parseErr != nil || recipient.Address != raw || strings.ContainsAny(raw, "\r\n") {
			return fmt.Errorf("invalid email recipient")
		}
		recipients = append(recipients, recipient)
	}
	subject, content, contentType, err := emailContent(config.Subject, config.Template, event)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(w.smtp.Address)
	if err != nil || host == "" {
		return fmt.Errorf("invalid SMTP address")
	}
	tlsMode := strings.ToLower(strings.TrimSpace(w.smtp.TLSMode))
	if tlsMode == "" {
		tlsMode = "starttls"
	}
	if tlsMode != "starttls" && tlsMode != "tls" && tlsMode != "none" {
		return fmt.Errorf("invalid SMTP TLS mode")
	}
	tlsConfig := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	var networkConnection net.Conn
	if tlsMode == "tls" {
		networkConnection, err = (&tls.Dialer{NetDialer: &net.Dialer{Timeout: notificationTimeout}, Config: tlsConfig}).DialContext(ctx, "tcp", w.smtp.Address)
	} else {
		networkConnection, err = (&net.Dialer{Timeout: notificationTimeout}).DialContext(ctx, "tcp", w.smtp.Address)
	}
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
	if tlsMode == "starttls" {
		if ok, _ := conn.Extension("STARTTLS"); !ok {
			return fmt.Errorf("SMTP server does not support STARTTLS")
		}
		if err = conn.StartTLS(tlsConfig); err != nil {
			return err
		}
	}
	if w.smtp.Username != "" {
		if err = conn.Auth(smtp.PlainAuth("", w.smtp.Username, w.smtp.Password, host)); err != nil {
			return err
		}
	}
	if err = conn.Mail(from.Address); err != nil {
		return err
	}
	for _, recipient := range recipients {
		if err = conn.Rcpt(recipient.Address); err != nil {
			return err
		}
	}
	writer, err := conn.Data()
	if err != nil {
		return err
	}
	toHeader := make([]string, 0, len(recipients))
	for _, recipient := range recipients {
		toHeader = append(toHeader, recipient.String())
	}
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: %s\r\n\r\n%s", from.String(), strings.Join(toHeader, ","), subject, contentType, content)
	if _, err = writer.Write([]byte(message)); err != nil {
		return err
	}
	return writer.Close()
}

func emailContent(subjectTemplate, bodyTemplate string, event store.OutboxEvent) (string, string, string, error) {
	subject := "Scheduler event " + event.Topic
	if subjectTemplate != "" {
		rendered, err := renderNotificationTemplate(subjectTemplate, event)
		if err != nil {
			return "", "", "", err
		}
		subject = rendered
	}
	if subject == "" || len(subject) > 998 || strings.ContainsAny(subject, "\r\n") {
		return "", "", "", fmt.Errorf("rendered email subject is invalid")
	}
	if bodyTemplate == "" {
		return subject, string(event.Payload), "application/json; charset=utf-8", nil
	}
	content, err := renderNotificationTemplate(bodyTemplate, event)
	if err != nil {
		return "", "", "", err
	}
	return subject, content, "text/plain; charset=utf-8", nil
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
