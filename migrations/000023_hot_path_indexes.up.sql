-- Keep the scheduler claim path proportional to active work rather than retained history.
CREATE INDEX job_runs_active_concurrency_idx
    ON job_runs(job_id, tenant_id) INCLUDE (lease_until)
    WHERE status IN ('running', 'waiting_callback');

CREATE INDEX job_runs_expired_lease_idx
    ON job_runs(lease_until, available_at, id)
    WHERE status = 'running';

-- Avoid full-table scans during hourly retention and per-run purge work.
CREATE INDEX job_run_idempotency_created_idx ON job_run_idempotency(created_at);
CREATE INDEX job_run_idempotency_run_idx ON job_run_idempotency(run_id);
CREATE INDEX outbox_published_idx ON outbox_events(published_at) WHERE published_at IS NOT NULL;
CREATE INDEX job_dependency_dispatches_created_idx ON job_dependency_dispatches(created_at);
CREATE INDEX job_dependency_dispatches_child_run_idx ON job_dependency_dispatches(child_run_id);
