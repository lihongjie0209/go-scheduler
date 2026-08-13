CREATE TABLE job_script_versions (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    script_language text NOT NULL CHECK (script_language IN ('shell', 'python')),
    script_source text NOT NULL CHECK (octet_length(script_source) BETWEEN 1 AND 1048576),
    remark text NOT NULL CHECK (char_length(remark) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, revision)
);

CREATE INDEX job_script_versions_tenant_job_created_idx
    ON job_script_versions(tenant_id, job_id, revision DESC);

INSERT INTO job_script_versions(id, tenant_id, job_id, revision, script_language, script_source, remark, created_at)
SELECT gen_random_uuid(), tenant_id, id, 1, script_language, script_source, 'initial version', updated_at
FROM jobs
WHERE script_language <> '' AND script_source <> '';
