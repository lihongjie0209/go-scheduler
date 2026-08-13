DROP INDEX IF EXISTS notification_channel_jobs_job_idx;
DROP TABLE IF EXISTS notification_channel_jobs;
ALTER TABLE notification_deliveries DROP COLUMN IF EXISTS dead_at;
ALTER TABLE notification_deliveries DROP CONSTRAINT IF EXISTS notification_deliveries_status_check;
ALTER TABLE notification_deliveries ADD CONSTRAINT notification_deliveries_status_check CHECK (status IN ('pending','delivered'));
ALTER TABLE notification_channels
    DROP CONSTRAINT IF EXISTS notification_channels_events_nonempty,
    DROP CONSTRAINT IF EXISTS notification_channels_backoff_order,
    DROP COLUMN IF EXISTS backoff_max_seconds,
    DROP COLUMN IF EXISTS backoff_initial_seconds,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS all_jobs,
    DROP COLUMN IF EXISTS event_types;
ALTER TABLE notification_channels DROP CONSTRAINT IF EXISTS notification_channels_kind_check;
ALTER TABLE notification_channels
    ADD CONSTRAINT notification_channels_kind_check CHECK (kind IN ('webhook','email'));
