ALTER TABLE notification_channels
    ADD COLUMN event_types text[] NOT NULL DEFAULT ARRAY['job.run.exhausted'],
    ADD COLUMN all_jobs boolean NOT NULL DEFAULT true,
    ADD COLUMN max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 100),
    ADD COLUMN backoff_initial_seconds integer NOT NULL DEFAULT 2 CHECK (backoff_initial_seconds BETWEEN 1 AND 3600),
    ADD COLUMN backoff_max_seconds integer NOT NULL DEFAULT 300 CHECK (backoff_max_seconds BETWEEN 1 AND 86400),
    ADD CONSTRAINT notification_channels_backoff_order CHECK (backoff_max_seconds >= backoff_initial_seconds),
    ADD CONSTRAINT notification_channels_events_nonempty CHECK (cardinality(event_types) > 0);

ALTER TABLE notification_channels DROP CONSTRAINT notification_channels_kind_check;
ALTER TABLE notification_channels
    ADD CONSTRAINT notification_channels_kind_check CHECK (kind IN ('webhook','email','dingtalk'));

CREATE TABLE notification_channel_jobs (
    channel_id uuid NOT NULL REFERENCES notification_channels(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (channel_id,job_id)
);

ALTER TABLE notification_deliveries DROP CONSTRAINT notification_deliveries_status_check;
ALTER TABLE notification_deliveries
    ADD CONSTRAINT notification_deliveries_status_check CHECK (status IN ('pending','delivered','dead')),
    ADD COLUMN dead_at timestamptz;

CREATE INDEX notification_channel_jobs_job_idx ON notification_channel_jobs(job_id,channel_id);
