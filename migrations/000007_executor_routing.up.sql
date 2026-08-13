CREATE TABLE executor_groups (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name text NOT NULL,
    route_strategy text NOT NULL CHECK (route_strategy IN ('first','last','round','random')),
    route_cursor bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id,name),
    UNIQUE (id,tenant_id)
);

CREATE TABLE executor_nodes (
    group_id uuid NOT NULL REFERENCES executor_groups(id) ON DELETE CASCADE,
    node_id text NOT NULL,
    address text NOT NULL,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id,node_id)
);
CREATE INDEX executor_nodes_live_idx ON executor_nodes(group_id,expires_at);

ALTER TABLE jobs ADD COLUMN executor_group_id uuid;
ALTER TABLE jobs ADD CONSTRAINT jobs_executor_group_tenant_fk
    FOREIGN KEY (executor_group_id,tenant_id) REFERENCES executor_groups(id,tenant_id) ON DELETE RESTRICT;
ALTER TABLE jobs ADD COLUMN executor_handler text NOT NULL DEFAULT '';
ALTER TABLE job_runs ADD COLUMN executor_node_id text;
ALTER TABLE job_runs ADD COLUMN executor_address text;
