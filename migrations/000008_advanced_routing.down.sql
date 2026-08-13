DROP TABLE IF EXISTS executor_job_route_state;
DROP TABLE IF EXISTS executor_job_route_counters;
ALTER TABLE executor_groups ADD COLUMN route_cursor bigint NOT NULL DEFAULT 0;
ALTER TABLE executor_groups DROP CONSTRAINT executor_groups_route_strategy_check;
ALTER TABLE executor_groups ADD CONSTRAINT executor_groups_route_strategy_check
    CHECK (route_strategy IN ('first','last','round','random'));
