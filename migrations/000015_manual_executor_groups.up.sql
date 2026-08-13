ALTER TABLE executor_groups
    ADD COLUMN registration_mode text NOT NULL DEFAULT 'automatic'
        CHECK (registration_mode IN ('automatic', 'manual'));

ALTER TABLE executor_nodes
    ADD COLUMN is_static boolean NOT NULL DEFAULT false;

CREATE INDEX executor_nodes_routable_idx
    ON executor_nodes(group_id, is_static, expires_at);
