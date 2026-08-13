package store

import (
	"context"
	"encoding/json"
	"errors"
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
	AllJobs, Enabled         bool
	JobIDs                   []string
	MaxAttempts              int
	BackoffInitialSeconds    int
	BackoffMaxSeconds        int
	Version                  int64
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
	if channel.AllJobs == (len(channel.JobIDs) > 0) {
		return NotificationChannel{}, ErrInvalidNotificationScope
	}
	channel.ID = uuid.NewString()
	storedConfig, encrypted, keyVersion, err := s.encodeNotificationConfig(channel.Config)
	if err != nil {
		return NotificationChannel{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("begin notification channel creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = validateNotificationChannelJobs(ctx, tx, channel); err != nil {
		return NotificationChannel{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO notification_channels(id,tenant_id,kind,name,config,encrypted_config,encryption_key_version,event_types,all_jobs,max_attempts,backoff_initial_seconds,backoff_max_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, channel.ID, channel.TenantID, channel.Kind, channel.Name, storedConfig, encrypted, keyVersion, channel.Events, channel.AllJobs, channel.MaxAttempts, channel.BackoffInitialSeconds, channel.BackoffMaxSeconds)
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
	channel.Enabled = true
	channel.Version = 1
	return channel, nil
}

func (s *Store) UpdateNotificationChannel(ctx context.Context, channel NotificationChannel) (NotificationChannel, error) {
	if channel.AllJobs == (len(channel.JobIDs) > 0) {
		return NotificationChannel{}, ErrInvalidNotificationScope
	}
	storedConfig, encrypted, keyVersion, err := s.encodeNotificationConfig(channel.Config)
	if err != nil {
		return NotificationChannel{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("begin notification channel update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = validateNotificationChannelJobs(ctx, tx, channel); err != nil {
		return NotificationChannel{}, err
	}
	err = tx.QueryRow(ctx, `UPDATE notification_channels SET kind=$4,name=$5,config=$6,encrypted_config=$7,encryption_key_version=$8,event_types=$9,all_jobs=$10,max_attempts=$11,backoff_initial_seconds=$12,backoff_max_seconds=$13,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 AND version=$3 AND deleted_at IS NULL RETURNING enabled,version`, channel.TenantID, channel.ID, channel.Version, channel.Kind, channel.Name, storedConfig, encrypted, keyVersion, channel.Events, channel.AllJobs, channel.MaxAttempts, channel.BackoffInitialSeconds, channel.BackoffMaxSeconds).Scan(&channel.Enabled, &channel.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return NotificationChannel{}, ErrConflict
	}
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("update notification channel: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM notification_channel_jobs WHERE channel_id=$1`, channel.ID); err != nil {
		return NotificationChannel{}, err
	}
	for _, jobID := range channel.JobIDs {
		if _, err = tx.Exec(ctx, `INSERT INTO notification_channel_jobs(channel_id,job_id) VALUES($1,$2)`, channel.ID, jobID); err != nil {
			return NotificationChannel{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return NotificationChannel{}, err
	}
	return channel, nil
}

func (s *Store) SetNotificationChannelEnabled(ctx context.Context, tenantID, id string, enabled bool, version int64) (NotificationChannel, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return NotificationChannel{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE notification_channels SET enabled=$4,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 AND version=$3 AND deleted_at IS NULL`, tenantID, id, version, enabled)
	if err != nil {
		return NotificationChannel{}, err
	}
	if tag.RowsAffected() != 1 {
		return NotificationChannel{}, ErrConflict
	}
	if !enabled {
		if err = terminalizePendingNotificationDeliveries(ctx, tx, id, "notification channel disabled"); err != nil {
			return NotificationChannel{}, err
		}
	}
	row := tx.QueryRow(ctx, `SELECT n.id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version,n.event_types,n.all_jobs,n.max_attempts,n.backoff_initial_seconds,n.backoff_max_seconds,n.enabled,n.version,COALESCE(array_agg(j.job_id::text ORDER BY j.job_id) FILTER (WHERE j.job_id IS NOT NULL),'{}') FROM notification_channels n LEFT JOIN notification_channel_jobs j ON j.channel_id=n.id WHERE n.tenant_id=$1 AND n.id=$2 AND n.deleted_at IS NULL GROUP BY n.id`, tenantID, id)
	channel, err := s.scanNotificationChannel(row, tenantID)
	if err != nil {
		return NotificationChannel{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return NotificationChannel{}, err
	}
	return channel, nil
}

func (s *Store) DeleteNotificationChannel(ctx context.Context, tenantID, id string, version int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE notification_channels SET enabled=false,deleted_at=now(),version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 AND version=$3 AND deleted_at IS NULL`, tenantID, id, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = terminalizePendingNotificationDeliveries(ctx, tx, id, "notification channel deleted"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) encodeNotificationConfig(config json.RawMessage) (json.RawMessage, []byte, *int, error) {
	if s.headerCipher == nil {
		return config, nil, nil, nil
	}
	ciphertext, keyVersion, err := s.headerCipher.Encrypt(config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encrypt notification config: %w", err)
	}
	return json.RawMessage(`{}`), ciphertext, &keyVersion, nil
}

func validateNotificationChannelJobs(ctx context.Context, tx pgx.Tx, channel NotificationChannel) error {
	if channel.AllJobs {
		return nil
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, channel.TenantID, channel.JobIDs).Scan(&count); err != nil {
		return fmt.Errorf("validate notification jobs: %w", err)
	}
	if count != len(channel.JobIDs) {
		return ErrNotFound
	}
	return nil
}

func terminalizePendingNotificationDeliveries(ctx context.Context, tx pgx.Tx, channelID, message string) error {
	_, err := tx.Exec(ctx, `WITH terminalized AS (
		UPDATE notification_deliveries SET status='dead',dead_at=now(),locked_by=NULL,locked_until=NULL,last_error=$2
		WHERE channel_id=$1 AND status='pending' RETURNING event_id
	), affected AS (SELECT DISTINCT event_id FROM terminalized)
	UPDATE outbox_events e SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL
	FROM affected a WHERE e.id=a.event_id AND NOT EXISTS (
		SELECT 1 FROM notification_deliveries pending WHERE pending.event_id=e.id AND pending.status='pending' AND pending.channel_id<>$1
	)`, channelID, boundedNotificationError(message))
	return err
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
	rows, err := s.pool.Query(ctx, `SELECT n.id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version,n.event_types,n.all_jobs,n.max_attempts,n.backoff_initial_seconds,n.backoff_max_seconds,n.enabled,n.version,COALESCE(array_agg(j.job_id::text ORDER BY j.job_id) FILTER (WHERE j.job_id IS NOT NULL),'{}') FROM notification_channels n LEFT JOIN notification_channel_jobs j ON j.channel_id=n.id WHERE n.tenant_id=$1 AND n.deleted_at IS NULL GROUP BY n.id ORDER BY n.created_at,n.id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NotificationChannel
	for rows.Next() {
		c, scanErr := s.scanNotificationChannel(rows, tenantID)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) NotificationChannel(ctx context.Context, tenantID, id string) (NotificationChannel, error) {
	row := s.pool.QueryRow(ctx, `SELECT n.id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version,n.event_types,n.all_jobs,n.max_attempts,n.backoff_initial_seconds,n.backoff_max_seconds,n.enabled,n.version,COALESCE(array_agg(j.job_id::text ORDER BY j.job_id) FILTER (WHERE j.job_id IS NOT NULL),'{}') FROM notification_channels n LEFT JOIN notification_channel_jobs j ON j.channel_id=n.id WHERE n.tenant_id=$1 AND n.id=$2 AND n.deleted_at IS NULL GROUP BY n.id`, tenantID, id)
	channel, err := s.scanNotificationChannel(row, tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return NotificationChannel{}, ErrNotFound
	}
	return channel, err
}

type notificationChannelScanner interface {
	Scan(dest ...any) error
}

func (s *Store) scanNotificationChannel(row notificationChannelScanner, tenantID string) (NotificationChannel, error) {
	var channel NotificationChannel
	var encrypted []byte
	var encryptionVersion *int
	channel.TenantID = tenantID
	if err := row.Scan(&channel.ID, &channel.Kind, &channel.Name, &channel.Config, &encrypted, &encryptionVersion, &channel.Events, &channel.AllJobs, &channel.MaxAttempts, &channel.BackoffInitialSeconds, &channel.BackoffMaxSeconds, &channel.Enabled, &channel.Version, &channel.JobIDs); err != nil {
		return NotificationChannel{}, err
	}
	if len(encrypted) == 0 {
		return channel, nil
	}
	if s.headerCipher == nil || encryptionVersion == nil {
		return NotificationChannel{}, fmt.Errorf("encrypted notification config requires store cipher")
	}
	plain, err := s.headerCipher.Decrypt(encrypted, *encryptionVersion)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("decrypt notification config: %w", err)
	}
	channel.Config = plain
	return channel, nil
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
		channelIDs, matchErr := matchingNotificationChannelIDs(ctx, tx, event.id, event.tenantID)
		if matchErr != nil {
			return matchErr
		}
		if len(channelIDs) == 0 {
			if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, event.id); err != nil {
				return err
			}
			continue
		}
		if _, err = tx.Exec(ctx, `INSERT INTO notification_deliveries(event_id,channel_id) SELECT $1,unnest($2::uuid[]) ON CONFLICT DO NOTHING`, event.id, channelIDs); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func matchingNotificationChannelIDs(ctx context.Context, tx pgx.Tx, eventID, tenantID string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT n.id FROM notification_channels n JOIN outbox_events e ON e.id=$1
		WHERE n.tenant_id=$2 AND n.enabled AND n.deleted_at IS NULL AND e.topic=ANY(n.event_types)
		AND (n.all_jobs OR EXISTS(SELECT 1 FROM notification_channel_jobs j WHERE j.channel_id=n.id AND j.job_id=NULLIF(e.payload->>'job_id','')::uuid))
		ORDER BY n.id FOR NO KEY UPDATE OF n`, eventID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
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

func (s *Store) NotificationHistory(ctx context.Context, tenantID, channelID, jobID, deliveryStatus string, beforeCreatedAt *time.Time, beforeID *string, limit int) ([]NotificationHistoryEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT d.id,d.event_id,n.id,n.name,n.kind,e.topic,COALESCE(e.payload->>'job_id',''),COALESCE(e.payload->>'run_id',''),d.status,d.attempts,COALESCE(d.last_error,''),d.created_at,d.delivered_at,d.dead_at
		FROM notification_deliveries d JOIN outbox_events e ON e.id=d.event_id JOIN notification_channels n ON n.id=d.channel_id
		WHERE e.tenant_id=$1 AND ($2='' OR n.id=$2::uuid) AND ($3='' OR e.payload->>'job_id'=$3) AND ($4='' OR d.status=$4)
		AND ($5::timestamptz IS NULL OR (d.created_at,d.id)<($5,$6::uuid))
		ORDER BY d.created_at DESC,d.id DESC LIMIT $7`, tenantID, channelID, jobID, deliveryStatus, beforeCreatedAt, beforeID, limit)
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
