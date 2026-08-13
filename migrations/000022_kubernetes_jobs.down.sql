DROP INDEX IF EXISTS jobs_kubernetes_cluster_idx;
ALTER TABLE jobs DROP CONSTRAINT jobs_script_definition_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_script_definition_check CHECK (
    (script_language = '' AND script_source = '') OR
    (script_language IN ('shell','python','nodejs','php','powershell') AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__script__') OR
    (script_language = 'docker' AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__docker__')
);
ALTER TABLE job_script_versions DROP CONSTRAINT job_script_versions_script_language_check;
ALTER TABLE job_script_versions ADD CONSTRAINT job_script_versions_script_language_check
    CHECK (script_language IN ('shell','python','nodejs','php','powershell','docker'));
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_kubernetes_cluster_fk;
ALTER TABLE jobs DROP COLUMN IF EXISTS kubernetes_cluster_id;
DROP TABLE IF EXISTS kubernetes_clusters;
