ALTER TABLE job_runs DROP CONSTRAINT job_runs_trigger_type_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_trigger_type_check
    CHECK (trigger_type IN ('schedule','manual','retry','dependency'));
ALTER TABLE job_runs ADD COLUMN parent_run_id uuid;
CREATE INDEX job_runs_parent_idx ON job_runs(parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE TABLE job_dependencies (
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    parent_job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    child_job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(parent_job_id,child_job_id),
    CHECK (parent_job_id <> child_job_id)
);
CREATE INDEX job_dependencies_child_idx ON job_dependencies(child_job_id);

CREATE TABLE job_dependency_dispatches (
    parent_run_id uuid NOT NULL,
    child_job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    child_run_id uuid NOT NULL,
    child_scheduled_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(parent_run_id,child_job_id)
);
