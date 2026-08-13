DROP INDEX IF EXISTS job_dependency_dispatches_child_run_idx;
DROP INDEX IF EXISTS job_dependency_dispatches_created_idx;
DROP INDEX IF EXISTS outbox_published_idx;
DROP INDEX IF EXISTS job_run_idempotency_run_idx;
DROP INDEX IF EXISTS job_run_idempotency_created_idx;
DROP INDEX IF EXISTS job_runs_expired_lease_idx;
DROP INDEX IF EXISTS job_runs_active_concurrency_idx;
