DROP TABLE IF EXISTS job_executor_labels;
ALTER TABLE executor_nodes DROP CONSTRAINT IF EXISTS executor_nodes_labels_check;
ALTER TABLE executor_nodes DROP COLUMN IF EXISTS labels;
