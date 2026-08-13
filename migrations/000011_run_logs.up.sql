CREATE TABLE job_run_logs (
    id bigserial PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    run_id uuid NOT NULL,
    entry_id text NOT NULL CHECK (length(entry_id) BETWEEN 1 AND 128),
    stream text NOT NULL CHECK (stream IN ('stdout','stderr')),
    content text NOT NULL CHECK (octet_length(content) <= 65536),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id,entry_id)
);

CREATE INDEX job_run_logs_read_idx ON job_run_logs(tenant_id,run_id,id);
CREATE INDEX job_run_logs_cleanup_idx ON job_run_logs(created_at);
