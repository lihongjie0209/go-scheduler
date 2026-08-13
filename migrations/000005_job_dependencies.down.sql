DROP TABLE IF EXISTS job_dependency_dispatches;
DROP TABLE IF EXISTS job_dependencies;
DROP INDEX IF EXISTS job_runs_parent_idx;
ALTER TABLE job_runs DROP COLUMN IF EXISTS parent_run_id;
ALTER TABLE job_runs DROP CONSTRAINT job_runs_trigger_type_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_trigger_type_check
    CHECK (trigger_type IN ('schedule','manual','retry'));
