DROP INDEX IF EXISTS job_runs_broadcast_group_idx;
ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS job_runs_shard_fields_check;
ALTER TABLE job_runs DROP COLUMN IF EXISTS shard_total;
ALTER TABLE job_runs DROP COLUMN IF EXISTS shard_index;
ALTER TABLE job_runs DROP COLUMN IF EXISTS broadcast_group_id;
ALTER TABLE executor_groups DROP CONSTRAINT executor_groups_route_strategy_check;
ALTER TABLE executor_groups ADD CONSTRAINT executor_groups_route_strategy_check
    CHECK (route_strategy IN ('first','last','round','random','hash','lfu','lru','failover','busyover'));
