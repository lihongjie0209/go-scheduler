DROP INDEX IF EXISTS job_runs_retry_of_idx;
ALTER TABLE job_runs DROP COLUMN IF EXISTS retry_of_run_id;
