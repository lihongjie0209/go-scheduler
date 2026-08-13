-- History reads start from tenant-scoped channels and then fetch their newest
-- deliveries. Keep this path proportional to a tenant's data, not the global
-- delivery table.
CREATE INDEX notification_deliveries_channel_history_idx
    ON notification_deliveries(channel_id, created_at DESC, id DESC);
