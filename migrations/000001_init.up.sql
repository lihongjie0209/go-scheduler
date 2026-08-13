CREATE EXTENSION IF NOT EXISTS pgcrypto;
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_partman') THEN
        BEGIN
            CREATE SCHEMA IF NOT EXISTS partman;
            CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
        EXCEPTION
            WHEN insufficient_privilege OR undefined_file THEN
                RAISE NOTICE 'pg_partman is unavailable; application-managed partitions will be used';
        END;
    END IF;
END
$migration$;

CREATE TABLE tenants (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    key_hash bytea NOT NULL UNIQUE,
    role text NOT NULL CHECK (role IN ('owner','admin','developer','viewer')),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    schedule_type text NOT NULL CHECK (schedule_type IN ('cron','once','fixed_interval')),
    schedule_expression text NOT NULL,
    timezone text NOT NULL DEFAULT 'UTC',
    target_url text NOT NULL,
    http_method text NOT NULL DEFAULT 'POST',
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    body_template text NOT NULL DEFAULT '',
    timeout_seconds integer NOT NULL DEFAULT 30 CHECK (timeout_seconds BETWEEN 1 AND 3600),
    max_retries integer NOT NULL DEFAULT 0 CHECK (max_retries BETWEEN 0 AND 20),
    overlap_policy text NOT NULL DEFAULT 'skip' CHECK (overlap_policy IN ('skip','queue','parallel')),
    misfire_policy text NOT NULL DEFAULT 'fire_once' CHECK (misfire_policy IN ('skip','fire_once','catch_up')),
    enabled boolean NOT NULL DEFAULT false,
    next_run_at timestamptz,
    last_run_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX jobs_due_idx ON jobs (next_run_at, id) WHERE enabled;

CREATE TABLE job_runs (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    trigger_type text NOT NULL CHECK (trigger_type IN ('schedule','manual','retry')),
    status text NOT NULL CHECK (status IN ('pending','running','waiting_callback','succeeded','failed','timed_out','cancelled','skipped')),
    attempt integer NOT NULL DEFAULT 1,
    scheduled_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_owner text,
    lease_until timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    response_status integer,
    response_body text,
    error_message text,
    runtime_input text NOT NULL DEFAULT '',
    idempotency_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(id,scheduled_at)
) PARTITION BY RANGE(scheduled_at);

DO $partition_setup$
DECLARE
    partition_start timestamptz;
    partition_end timestamptz;
    partition_name text;
    offset_month integer;
	use_partman boolean := false;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') THEN
        BEGIN
            PERFORM partman.create_parent(
                p_parent_table := 'public.job_runs',
                p_control := 'scheduled_at',
                p_interval := '1 month',
                p_premake := 3,
                p_default_table := true,
                p_automatic_maintenance := 'on'
            );
            UPDATE partman.part_config
            SET retention = '90 days',
                retention_keep_table = false,
                retention_keep_index = false
            WHERE parent_table = 'public.job_runs';
            use_partman := true;
        EXCEPTION
            WHEN insufficient_privilege OR undefined_function THEN
                RAISE NOTICE 'pg_partman cannot manage job_runs; application-managed partitions will be used';
        END;
    END IF;
    IF NOT use_partman THEN
        FOR offset_month IN -3..3 LOOP
            partition_start := date_trunc('month', now()) + make_interval(months => offset_month);
            partition_end := partition_start + interval '1 month';
            partition_name := format('job_runs_y%sm%s', to_char(partition_start, 'YYYY'), to_char(partition_start, 'MM'));
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF job_runs FOR VALUES FROM (%L) TO (%L)',
                partition_name, partition_start, partition_end
            );
        END LOOP;
        CREATE TABLE job_runs_default PARTITION OF job_runs DEFAULT;
    END IF;
END
$partition_setup$;

CREATE TABLE job_run_idempotency (
    tenant_id uuid NOT NULL,
    job_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    run_id uuid NOT NULL,
    scheduled_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,job_id,idempotency_key)
);

CREATE INDEX job_runs_claim_idx ON job_runs (available_at, scheduled_at) WHERE status = 'pending';
CREATE INDEX job_runs_job_idx ON job_runs (tenant_id, job_id, scheduled_at DESC);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    available_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);
