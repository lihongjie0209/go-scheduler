ALTER TABLE executor_groups DROP CONSTRAINT executor_groups_route_strategy_check;
ALTER TABLE executor_groups ADD CONSTRAINT executor_groups_route_strategy_check
    CHECK (route_strategy IN ('first','last','round','random','hash','lfu','lru'));
