ALTER TABLE job_runs ADD COLUMN retry_of_run_id uuid;
CREATE INDEX job_runs_retry_of_idx ON job_runs(retry_of_run_id) WHERE retry_of_run_id IS NOT NULL;
