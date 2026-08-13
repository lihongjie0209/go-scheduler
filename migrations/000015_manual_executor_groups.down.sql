DROP INDEX IF EXISTS executor_nodes_routable_idx;
ALTER TABLE executor_nodes DROP COLUMN is_static;
ALTER TABLE executor_groups DROP COLUMN registration_mode;
