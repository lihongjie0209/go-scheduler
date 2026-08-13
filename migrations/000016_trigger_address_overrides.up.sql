ALTER TABLE job_runs
    ADD COLUMN override_addresses text[] NOT NULL DEFAULT '{}';

CREATE TABLE executor_override_route_state (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    address text NOT NULL,
    use_count bigint NOT NULL DEFAULT 0,
    last_used_at timestamptz NOT NULL DEFAULT '-infinity',
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, address)
);
