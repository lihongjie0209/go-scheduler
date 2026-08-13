DROP TABLE IF EXISTS refresh_sessions;
ALTER TABLE notification_channels DROP COLUMN IF EXISTS encryption_key_version;
ALTER TABLE notification_channels DROP COLUMN IF EXISTS encrypted_config;
