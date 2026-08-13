ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_docker_registry_auth_encryption_check;
ALTER TABLE jobs
    DROP COLUMN IF EXISTS docker_registry_auth_key_version,
    DROP COLUMN IF EXISTS encrypted_docker_registry_auth;
