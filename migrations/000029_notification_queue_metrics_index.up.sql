CREATE INDEX notification_deliveries_pending_created_idx
    ON notification_deliveries(created_at)
    WHERE status = 'pending';
