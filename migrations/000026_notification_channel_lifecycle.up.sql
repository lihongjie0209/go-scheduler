ALTER TABLE notification_channels
    ADD COLUMN version bigint NOT NULL DEFAULT 1,
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN deleted_at timestamptz,
    ADD CONSTRAINT notification_channels_version_positive CHECK (version > 0);

CREATE INDEX notification_channels_tenant_active_idx
    ON notification_channels(tenant_id, created_at, id)
    WHERE deleted_at IS NULL;
