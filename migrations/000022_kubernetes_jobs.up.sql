CREATE TABLE kubernetes_clusters (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    auth_mode text NOT NULL CHECK (auth_mode IN ('kubeconfig','service_account')),
    api_server text NOT NULL DEFAULT '',
    namespace text NOT NULL DEFAULT 'default',
    encrypted_credentials bytea NOT NULL,
    encryption_key_version integer NOT NULL,
    insecure_skip_tls_verify boolean NOT NULL DEFAULT false,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, id)
);

ALTER TABLE jobs ADD COLUMN kubernetes_cluster_id uuid;
ALTER TABLE jobs ADD CONSTRAINT jobs_kubernetes_cluster_fk
    FOREIGN KEY (tenant_id, kubernetes_cluster_id) REFERENCES kubernetes_clusters(tenant_id, id) ON DELETE RESTRICT;

ALTER TABLE jobs DROP CONSTRAINT jobs_script_definition_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_script_definition_check CHECK (
    (script_language = '' AND script_source = '' AND kubernetes_cluster_id IS NULL) OR
    (script_language IN ('shell','python','nodejs','php','powershell') AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__script__' AND kubernetes_cluster_id IS NULL) OR
    (script_language = 'docker' AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__docker__' AND kubernetes_cluster_id IS NULL) OR
    (script_language = 'kubernetes' AND script_source <> '' AND executor_group_id IS NOT NULL AND executor_handler = '__kubernetes__' AND kubernetes_cluster_id IS NOT NULL)
);

ALTER TABLE job_script_versions DROP CONSTRAINT job_script_versions_script_language_check;
ALTER TABLE job_script_versions ADD CONSTRAINT job_script_versions_script_language_check
    CHECK (script_language IN ('shell','python','nodejs','php','powershell','docker','kubernetes'));

CREATE INDEX jobs_kubernetes_cluster_idx ON jobs(kubernetes_cluster_id) WHERE kubernetes_cluster_id IS NOT NULL;
