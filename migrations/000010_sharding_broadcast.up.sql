ALTER TABLE executor_groups DROP CONSTRAINT executor_groups_route_strategy_check;
ALTER TABLE executor_groups ADD CONSTRAINT executor_groups_route_strategy_check
    CHECK (route_strategy IN ('first','last','round','random','hash','lfu','lru','failover','busyover','sharding_broadcast'));

ALTER TABLE job_runs ADD COLUMN broadcast_group_id uuid;
ALTER TABLE job_runs ADD COLUMN shard_index integer;
ALTER TABLE job_runs ADD COLUMN shard_total integer;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_shard_fields_check CHECK (
    (broadcast_group_id IS NULL AND shard_index IS NULL AND shard_total IS NULL)
    OR
    (broadcast_group_id IS NOT NULL AND shard_index >= 0 AND shard_total > 0 AND shard_index < shard_total)
);
CREATE INDEX job_runs_broadcast_group_idx ON job_runs(tenant_id,broadcast_group_id,shard_index) WHERE broadcast_group_id IS NOT NULL;
