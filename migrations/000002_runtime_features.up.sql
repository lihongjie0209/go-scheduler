ALTER TABLE tenants ADD COLUMN max_concurrent_runs integer NOT NULL DEFAULT 100 CHECK (max_concurrent_runs > 0);
ALTER TABLE jobs ADD COLUMN max_concurrent_runs integer NOT NULL DEFAULT 1 CHECK (max_concurrent_runs > 0);
ALTER TABLE jobs ADD COLUMN max_queue_size integer NOT NULL DEFAULT 1000 CHECK (max_queue_size BETWEEN 1 AND 100000);
ALTER TABLE jobs ADD COLUMN max_catch_up integer NOT NULL DEFAULT 10 CHECK (max_catch_up BETWEEN 1 AND 1000);
ALTER TABLE jobs ADD COLUMN callback_timeout_seconds integer NOT NULL DEFAULT 3600 CHECK (callback_timeout_seconds BETWEEN 1 AND 86400);
ALTER TABLE jobs ADD COLUMN encrypted_headers bytea;
ALTER TABLE jobs ADD COLUMN encryption_key_version integer;

ALTER TABLE job_runs ADD COLUMN callback_token_hash bytea;
ALTER TABLE job_runs ADD COLUMN callback_deadline timestamptz;
ALTER TABLE job_runs ADD COLUMN callback_consumed_at timestamptz;
CREATE INDEX job_runs_callback_timeout_idx ON job_runs(callback_deadline) WHERE status='waiting_callback';

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    platform_admin boolean NOT NULL DEFAULT false,
    disabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant_memberships (
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('owner','admin','developer','viewer')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,user_id)
);

CREATE TABLE notification_channels (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('webhook','email')),
    name text NOT NULL,
    config jsonb NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE outbox_events ADD COLUMN locked_by text;
ALTER TABLE outbox_events ADD COLUMN locked_until timestamptz;
ALTER TABLE outbox_events ADD COLUMN last_error text;
CREATE INDEX outbox_claim_idx ON outbox_events(available_at) WHERE published_at IS NULL;
