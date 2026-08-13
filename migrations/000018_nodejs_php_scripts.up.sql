ALTER TABLE jobs DROP CONSTRAINT jobs_script_definition_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_script_definition_check CHECK (
    (script_language = '' AND script_source = '') OR
    (script_language IN ('shell','python','nodejs','php') AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__script__')
);

ALTER TABLE job_script_versions DROP CONSTRAINT job_script_versions_script_language_check;
ALTER TABLE job_script_versions ADD CONSTRAINT job_script_versions_script_language_check
    CHECK (script_language IN ('shell','python','nodejs','php'));
