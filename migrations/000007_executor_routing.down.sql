ALTER TABLE job_runs DROP COLUMN IF EXISTS executor_address;
ALTER TABLE job_runs DROP COLUMN IF EXISTS executor_node_id;
ALTER TABLE jobs DROP COLUMN IF EXISTS executor_handler;
ALTER TABLE jobs DROP COLUMN IF EXISTS executor_group_id;
DROP TABLE IF EXISTS executor_nodes;
DROP TABLE IF EXISTS executor_groups;
