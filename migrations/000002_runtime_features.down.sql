DROP TABLE IF EXISTS notification_channels;
DROP TABLE IF EXISTS tenant_memberships;
DROP TABLE IF EXISTS users;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS last_error, DROP COLUMN IF EXISTS locked_until, DROP COLUMN IF EXISTS locked_by;
ALTER TABLE job_runs DROP COLUMN IF EXISTS callback_consumed_at, DROP COLUMN IF EXISTS callback_deadline, DROP COLUMN IF EXISTS callback_token_hash;
ALTER TABLE jobs DROP COLUMN IF EXISTS encryption_key_version, DROP COLUMN IF EXISTS encrypted_headers, DROP COLUMN IF EXISTS callback_timeout_seconds, DROP COLUMN IF EXISTS max_catch_up, DROP COLUMN IF EXISTS max_queue_size, DROP COLUMN IF EXISTS max_concurrent_runs;
ALTER TABLE tenants DROP COLUMN IF EXISTS max_concurrent_runs;
