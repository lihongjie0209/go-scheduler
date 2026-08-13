DROP INDEX IF EXISTS notification_channels_tenant_active_idx;
ALTER TABLE notification_channels
    DROP CONSTRAINT IF EXISTS notification_channels_version_positive,
    DROP COLUMN IF EXISTS deleted_at,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS version;
