ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_type_check
    CHECK (schedule_type IN ('cron','once','fixed_interval','fixed_rate','fixed_delay'));

ALTER TABLE job_runs ADD COLUMN reschedule_on_terminal boolean NOT NULL DEFAULT false;
