package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxEvent struct {
	ID, TenantID, Topic string
	Payload             json.RawMessage
	Attempts            int
}
type NotificationChannel struct {
	ID, TenantID, Kind, Name string
	Config                   json.RawMessage
	Events                   []string
	AllJobs                  bool
	JobIDs                   []string
	MaxAttempts              int
	BackoffInitialSeconds    int
	BackoffMaxSeconds        int
}
type NotificationDelivery struct {
	ID, EventID string
	Event       OutboxEvent
	Channel     NotificationChannel
	Attempts    int
}

type NotificationHistoryEntry struct {
	DeliveryID, EventID, ChannelID, ChannelName, ChannelKind string
	Topic, JobID, RunID, Status, LastError                   string
	Attempts                                                 int
	CreatedAt                                                time.Time
	DeliveredAt, DeadAt                                      *time.Time
}

const maxNotificationErrorBytes = 4096

const runLifecycleEventSQL = `INSERT INTO outbox_events(id,tenant_id,topic,payload)
		SELECT $4,tenant_id,'job.run.'||$3,jsonb_build_object(
			'run_id',id::text,'job_id',job_id::text,'tenant_id',tenant_id::text,
			'status',status,'attempt',attempt,'trigger_type',trigger_type,
			'scheduled_at',scheduled_at,'started_at',started_at,'finished_at',finished_at,
			'response_status',response_status,'error',COALESCE(error_message,''),'occurred_at',now())
		FROM job_runs r WHERE r.id=$1 AND r.status=$2`

func emitRunLifecycleEventTx(ctx context.Context, tx pgx.Tx, runID, runStatus string) error {
	return emitRunEventTx(ctx, tx, runID, runStatus, runStatus)
}

func emitRunEventTx(ctx context.Context, tx pgx.Tx, runID, expectedStatus, eventType string) error {
	_, err := tx.Exec(ctx, runLifecycleEventSQL, runID, expectedStatus, eventType, uuid.NewString())
	if err != nil {
		return fmt.Errorf("emit run %s event: %w", eventType, err)
	}
	// The event is persisted independently of current subscribers so lifecycle
	// history remains auditable and delivery matching can happen asynchronously.
	return nil
}

func (s *Store) CreateNotificationChannel(ctx context.Context, channel NotificationChannel) (NotificationChannel, error) {
	channel.ID = uuid.NewString()
	storedConfig := channel.Config
	var encrypted []byte
	var version *int
	if s.headerCipher != nil {
		ciphertext, keyVersion, err := s.headerCipher.Encrypt(channel.Config)
		if err != nil {
			return NotificationChannel{}, fmt.Errorf("encrypt notification config: %w", err)
		}
		encrypted = ciphertext
		version = &keyVersion
		storedConfig = json.RawMessage(`{}`)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("begin notification channel creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if !channel.AllJobs {
		var count int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, channel.TenantID, channel.JobIDs).Scan(&count); err != nil {
			return NotificationChannel{}, fmt.Errorf("validate notification jobs: %w", err)
		}
		if count != len(channel.JobIDs) {
			return NotificationChannel{}, ErrNotFound
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO notification_channels(id,tenant_id,kind,name,config,encrypted_config,encryption_key_version,event_types,all_jobs,max_attempts,backoff_initial_seconds,backoff_max_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, channel.ID, channel.TenantID, channel.Kind, channel.Name, storedConfig, encrypted, version, channel.Events, channel.AllJobs, channel.MaxAttempts, channel.BackoffInitialSeconds, channel.BackoffMaxSeconds)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("create notification channel: %w", err)
	}
	for _, jobID := range channel.JobIDs {
		if _, err = tx.Exec(ctx, `INSERT INTO notification_channel_jobs(channel_id,job_id) VALUES($1,$2)`, channel.ID, jobID); err != nil {
			return NotificationChannel{}, fmt.Errorf("bind notification job: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return NotificationChannel{}, fmt.Errorf("commit notification channel creation: %w", err)
	}
	return channel, nil
}

func (s *Store) ClaimOutbox(ctx context.Context, owner string, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `WITH picked AS (SELECT id FROM outbox_events WHERE published_at IS NULL AND available_at<=now() AND (locked_until IS NULL OR locked_until<now()) ORDER BY available_at FOR UPDATE SKIP LOCKED LIMIT $1), claimed AS (UPDATE outbox_events e SET locked_by=$2,locked_until=now()+interval '30 seconds',attempts=attempts+1 FROM picked WHERE e.id=picked.id RETURNING e.id,e.tenant_id,e.topic,e.payload,e.attempts) SELECT id,tenant_id,topic,payload,attempts FROM claimed`, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err = rows.Scan(&e.ID, &e.TenantID, &e.Topic, &e.Payload, &e.Attempts); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
func (s *Store) NotificationChannels(ctx context.Context, tenantID string) ([]NotificationChannel, error) {
	rows, err := s.pool.Query(ctx, `SELECT n.id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version,n.event_types,n.all_jobs,n.max_attempts,n.backoff_initial_seconds,n.backoff_max_seconds,COALESCE(array_agg(j.job_id::text ORDER BY j.job_id) FILTER (WHERE j.job_id IS NOT NULL),'{}') FROM notification_channels n LEFT JOIN notification_channel_jobs j ON j.channel_id=n.id WHERE n.tenant_id=$1 AND n.enabled GROUP BY n.id ORDER BY n.created_at,n.id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NotificationChannel
	for rows.Next() {
		var c NotificationChannel
		var encrypted []byte
		var version *int
		c.TenantID = tenantID
		if err = rows.Scan(&c.ID, &c.Kind, &c.Name, &c.Config, &encrypted, &version, &c.Events, &c.AllJobs, &c.MaxAttempts, &c.BackoffInitialSeconds, &c.BackoffMaxSeconds, &c.JobIDs); err != nil {
			return nil, err
		}
		if len(encrypted) > 0 {
			if s.headerCipher == nil || version == nil {
				return nil, fmt.Errorf("encrypted notification config requires store cipher")
			}
			plain, decryptErr := s.headerCipher.Decrypt(encrypted, *version)
			if decryptErr != nil {
				return nil, fmt.Errorf("decrypt notification config: %w", decryptErr)
			}
			c.Config = plain
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) PrepareNotificationDeliveries(ctx context.Context, limit int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT e.id,e.tenant_id FROM outbox_events e WHERE e.published_at IS NULL AND e.available_at<=now() AND NOT EXISTS(SELECT 1 FROM notification_deliveries d WHERE d.event_id=e.id) ORDER BY e.available_at,e.id FOR UPDATE OF e SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return err
	}
	type eventRef struct{ id, tenantID string }
	var events []eventRef
	for rows.Next() {
		var event eventRef
		if err = rows.Scan(&event.id, &event.tenantID); err != nil {
			rows.Close()
			return err
		}
		events = append(events, event)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, event := range events {
		tag, insertErr := tx.Exec(ctx, `INSERT INTO notification_deliveries(event_id,channel_id)
			SELECT e.id,n.id FROM notification_channels n JOIN outbox_events e ON e.id=$1
			WHERE n.tenant_id=$2 AND n.enabled AND e.topic=ANY(n.event_types)
			AND (n.all_jobs OR EXISTS(SELECT 1 FROM notification_channel_jobs j WHERE j.channel_id=n.id AND j.job_id=NULLIF(e.payload->>'job_id','')::uuid))
			ON CONFLICT DO NOTHING`, event.id, event.tenantID)
		if insertErr != nil {
			return insertErr
		}
		if tag.RowsAffected() == 0 {
			if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, event.id); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) ClaimNotificationDeliveries(ctx context.Context, owner string, limit int) ([]NotificationDelivery, error) {
	rows, err := s.pool.Query(ctx, `WITH picked AS (SELECT id FROM notification_deliveries WHERE status='pending' AND available_at<=now() AND (locked_until IS NULL OR locked_until<now()) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1), claimed AS (UPDATE notification_deliveries d SET locked_by=$2,locked_until=now()+interval '30 seconds',attempts=attempts+1 FROM picked WHERE d.id=picked.id RETURNING d.id,d.event_id,d.channel_id,d.attempts) SELECT c.id,c.event_id,c.attempts,e.id,e.tenant_id,e.topic,e.payload,e.attempts,n.id,n.tenant_id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version,n.max_attempts,n.backoff_initial_seconds,n.backoff_max_seconds FROM claimed c JOIN outbox_events e ON e.id=c.event_id JOIN notification_channels n ON n.id=c.channel_id`, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim notification deliveries: %w", err)
	}
	defer rows.Close()
	var deliveries []NotificationDelivery
	for rows.Next() {
		var d NotificationDelivery
		var encrypted []byte
		var version *int
		if err = rows.Scan(&d.ID, &d.EventID, &d.Attempts, &d.Event.ID, &d.Event.TenantID, &d.Event.Topic, &d.Event.Payload, &d.Event.Attempts, &d.Channel.ID, &d.Channel.TenantID, &d.Channel.Kind, &d.Channel.Name, &d.Channel.Config, &encrypted, &version, &d.Channel.MaxAttempts, &d.Channel.BackoffInitialSeconds, &d.Channel.BackoffMaxSeconds); err != nil {
			return nil, err
		}
		if len(encrypted) > 0 {
			if s.headerCipher == nil || version == nil {
				return nil, fmt.Errorf("encrypted notification config requires store cipher")
			}
			d.Channel.Config, err = s.headerCipher.Decrypt(encrypted, *version)
			if err != nil {
				return nil, err
			}
		}
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

func (s *Store) CompleteNotificationDelivery(ctx context.Context, owner, deliveryID, eventID string) error {
	return s.finishNotificationDelivery(ctx, owner, deliveryID, eventID, "delivered", "")
}

func (s *Store) DeadLetterNotificationDelivery(ctx context.Context, owner, deliveryID, eventID, message string) error {
	return s.finishNotificationDelivery(ctx, owner, deliveryID, eventID, "dead", boundedNotificationError(message))
}

func (s *Store) finishNotificationDelivery(ctx context.Context, owner, deliveryID, eventID, deliveryStatus, message string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE notification_deliveries SET status=$4,delivered_at=CASE WHEN $4='delivered' THEN now() ELSE NULL END,dead_at=CASE WHEN $4='dead' THEN now() ELSE NULL END,locked_by=NULL,locked_until=NULL,last_error=NULLIF($5,'') WHERE id=$1 AND event_id=$2 AND status='pending' AND locked_by=$3 AND locked_until>=now()`, deliveryID, eventID, owner, deliveryStatus, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNotificationLeaseLost
	}
	var pending bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notification_deliveries WHERE event_id=$1 AND status='pending')`, eventID).Scan(&pending); err != nil {
		return err
	}
	if !pending {
		if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, eventID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) NotificationHistory(ctx context.Context, tenantID, channelID, jobID, deliveryStatus string, limit int) ([]NotificationHistoryEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT d.id,d.event_id,n.id,n.name,n.kind,e.topic,COALESCE(e.payload->>'job_id',''),COALESCE(e.payload->>'run_id',''),d.status,d.attempts,COALESCE(d.last_error,''),d.created_at,d.delivered_at,d.dead_at
		FROM notification_deliveries d JOIN outbox_events e ON e.id=d.event_id JOIN notification_channels n ON n.id=d.channel_id
		WHERE e.tenant_id=$1 AND ($2='' OR n.id=$2::uuid) AND ($3='' OR e.payload->>'job_id'=$3) AND ($4='' OR d.status=$4)
		ORDER BY d.created_at DESC,d.id DESC LIMIT $5`, tenantID, channelID, jobID, deliveryStatus, limit)
	if err != nil {
		return nil, fmt.Errorf("list notification history: %w", err)
	}
	defer rows.Close()
	history := make([]NotificationHistoryEntry, 0)
	for rows.Next() {
		var entry NotificationHistoryEntry
		if err = rows.Scan(&entry.DeliveryID, &entry.EventID, &entry.ChannelID, &entry.ChannelName, &entry.ChannelKind, &entry.Topic, &entry.JobID, &entry.RunID, &entry.Status, &entry.Attempts, &entry.LastError, &entry.CreatedAt, &entry.DeliveredAt, &entry.DeadAt); err != nil {
			return nil, err
		}
		history = append(history, entry)
	}
	return history, rows.Err()
}

func (s *Store) RetryNotificationDelivery(ctx context.Context, owner, id, message string, delay time.Duration) error {
	tag, err := s.pool.Exec(ctx, `UPDATE notification_deliveries SET available_at=now()+$3*interval '1 second',locked_by=NULL,locked_until=NULL,last_error=$4 WHERE id=$1 AND status='pending' AND locked_by=$2 AND locked_until>=now()`, id, owner, delay.Seconds(), boundedNotificationError(message))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNotificationLeaseLost
	}
	return nil
}

func boundedNotificationError(message string) string {
	message = strings.ToValidUTF8(message, "�")
	if len(message) <= maxNotificationErrorBytes {
		return message
	}
	limit := maxNotificationErrorBytes
	for limit > 0 && message[limit]&0xc0 == 0x80 {
		limit--
	}
	return message[:limit]
}
func (s *Store) PublishOutbox(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, id)
	return err
}
func (s *Store) RetryOutbox(ctx context.Context, id, message string, delay time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET available_at=now()+$2*interval '1 second',locked_by=NULL,locked_until=NULL,last_error=$3 WHERE id=$1`, id, delay.Seconds(), message)
	return err
}
