ALTER TABLE job_runs DROP COLUMN IF EXISTS reschedule_on_terminal;

ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_type_check
    CHECK (schedule_type IN ('cron','once','fixed_interval'));
