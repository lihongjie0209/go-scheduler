ALTER TABLE job_runs ADD COLUMN external_execution_id uuid;
UPDATE job_runs SET external_execution_id = id;
ALTER TABLE job_runs ALTER COLUMN external_execution_id SET DEFAULT gen_random_uuid();
ALTER TABLE job_runs ALTER COLUMN external_execution_id SET NOT NULL;
