ALTER TABLE jobs
    ADD COLUMN encrypted_docker_registry_auth bytea,
    ADD COLUMN docker_registry_auth_key_version integer;

ALTER TABLE jobs ADD CONSTRAINT jobs_docker_registry_auth_encryption_check CHECK (
    (encrypted_docker_registry_auth IS NULL AND docker_registry_auth_key_version IS NULL) OR
    (encrypted_docker_registry_auth IS NOT NULL AND docker_registry_auth_key_version IS NOT NULL)
);
