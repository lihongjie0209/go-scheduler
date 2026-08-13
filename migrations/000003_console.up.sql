CREATE TABLE refresh_sessions (
    id uuid PRIMARY KEY,
    family_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    revoked_at timestamptz,
    replaced_by uuid,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX refresh_sessions_user_idx ON refresh_sessions(user_id, expires_at DESC);
CREATE INDEX refresh_sessions_family_idx ON refresh_sessions(family_id);

ALTER TABLE notification_channels ADD COLUMN encrypted_config bytea;
ALTER TABLE notification_channels ADD COLUMN encryption_key_version integer;
