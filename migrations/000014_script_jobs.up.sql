ALTER TABLE jobs
    ADD COLUMN script_language text NOT NULL DEFAULT '',
    ADD COLUMN script_source text NOT NULL DEFAULT '';

ALTER TABLE jobs ADD CONSTRAINT jobs_script_definition_check CHECK (
    (script_language = '' AND script_source = '') OR
    (script_language IN ('shell','python') AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__script__')
);
