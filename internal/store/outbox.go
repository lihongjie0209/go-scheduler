package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	ID, TenantID, Topic string
	Payload             json.RawMessage
	Attempts            int
}
type NotificationChannel struct {
	ID, TenantID, Kind, Name string
	Config                   json.RawMessage
}
type NotificationDelivery struct {
	ID, EventID string
	Event       OutboxEvent
	Channel     NotificationChannel
	Attempts    int
}

func (s *Store) CreateNotificationChannel(ctx context.Context, tenantID, kind, name string, config json.RawMessage) (NotificationChannel, error) {
	channel := NotificationChannel{ID: uuid.NewString(), TenantID: tenantID, Kind: kind, Name: name, Config: config}
	var encrypted []byte
	var version *int
	if s.headerCipher != nil {
		ciphertext, keyVersion, err := s.headerCipher.Encrypt(config)
		if err != nil {
			return NotificationChannel{}, fmt.Errorf("encrypt notification config: %w", err)
		}
		encrypted = ciphertext
		version = &keyVersion
		config = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO notification_channels(id,tenant_id,kind,name,config,encrypted_config,encryption_key_version) VALUES($1,$2,$3,$4,$5,$6,$7)`, channel.ID, tenantID, kind, name, config, encrypted, version)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("create notification channel: %w", err)
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
	rows, err := s.pool.Query(ctx, `SELECT id,kind,name,config,encrypted_config,encryption_key_version FROM notification_channels WHERE tenant_id=$1 AND enabled`, tenantID)
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
		if err = rows.Scan(&c.ID, &c.Kind, &c.Name, &c.Config, &encrypted, &version); err != nil {
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
		tag, insertErr := tx.Exec(ctx, `INSERT INTO notification_deliveries(event_id,channel_id) SELECT $1,id FROM notification_channels WHERE tenant_id=$2 AND enabled ON CONFLICT DO NOTHING`, event.id, event.tenantID)
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
	rows, err := s.pool.Query(ctx, `WITH picked AS (SELECT id FROM notification_deliveries WHERE status='pending' AND available_at<=now() AND (locked_until IS NULL OR locked_until<now()) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1), claimed AS (UPDATE notification_deliveries d SET locked_by=$2,locked_until=now()+interval '30 seconds',attempts=attempts+1 FROM picked WHERE d.id=picked.id RETURNING d.id,d.event_id,d.channel_id,d.attempts) SELECT c.id,c.event_id,c.attempts,e.id,e.tenant_id,e.topic,e.payload,e.attempts,n.id,n.tenant_id,n.kind,n.name,n.config,n.encrypted_config,n.encryption_key_version FROM claimed c JOIN outbox_events e ON e.id=c.event_id JOIN notification_channels n ON n.id=c.channel_id`, limit, owner)
	if err != nil {
		return nil, fmt.Errorf("claim notification deliveries: %w", err)
	}
	defer rows.Close()
	var deliveries []NotificationDelivery
	for rows.Next() {
		var d NotificationDelivery
		var encrypted []byte
		var version *int
		if err = rows.Scan(&d.ID, &d.EventID, &d.Attempts, &d.Event.ID, &d.Event.TenantID, &d.Event.Topic, &d.Event.Payload, &d.Event.Attempts, &d.Channel.ID, &d.Channel.TenantID, &d.Channel.Kind, &d.Channel.Name, &d.Channel.Config, &encrypted, &version); err != nil {
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

func (s *Store) CompleteNotificationDelivery(ctx context.Context, deliveryID, eventID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `UPDATE notification_deliveries SET status='delivered',delivered_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1 AND event_id=$2 AND status='pending'`, deliveryID, eventID); err != nil {
		return err
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

func (s *Store) RetryNotificationDelivery(ctx context.Context, id, message string, delay time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE notification_deliveries SET available_at=now()+$2*interval '1 second',locked_by=NULL,locked_until=NULL,last_error=$3 WHERE id=$1 AND status='pending'`, id, delay.Seconds(), message)
	return err
}
func (s *Store) PublishOutbox(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, id)
	return err
}
func (s *Store) RetryOutbox(ctx context.Context, id, message string, delay time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET available_at=now()+$2*interval '1 second',locked_by=NULL,locked_until=NULL,last_error=$3 WHERE id=$1`, id, delay.Seconds(), message)
	return err
}
