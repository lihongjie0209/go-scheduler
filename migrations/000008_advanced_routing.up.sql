ALTER TABLE executor_groups DROP CONSTRAINT executor_groups_route_strategy_check;
ALTER TABLE executor_groups ADD CONSTRAINT executor_groups_route_strategy_check
    CHECK (route_strategy IN ('first','last','round','random','hash','lfu','lru'));
ALTER TABLE executor_groups DROP COLUMN route_cursor;

CREATE TABLE executor_job_route_counters (
    job_id uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    route_count bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE executor_job_route_state (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    group_id uuid NOT NULL REFERENCES executor_groups(id) ON DELETE CASCADE,
    node_id text NOT NULL,
    use_count bigint NOT NULL DEFAULT 0,
    last_used_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id,node_id),
    FOREIGN KEY (group_id,node_id) REFERENCES executor_nodes(group_id,node_id) ON DELETE CASCADE
);
CREATE INDEX executor_job_route_state_group_idx ON executor_job_route_state(group_id,node_id);
