ALTER TABLE executor_nodes
    ADD COLUMN labels text[] NOT NULL DEFAULT '{}';

ALTER TABLE executor_nodes ADD CONSTRAINT executor_nodes_labels_check CHECK (
    cardinality(labels) <= 32 AND
    array_position(labels, NULL) IS NULL AND
    (cardinality(labels) = 0 OR array_to_string(labels, ',') ~ '^[a-z0-9][a-z0-9._-]{0,62}(,[a-z0-9][a-z0-9._-]{0,62})*$')
);

CREATE TABLE job_executor_labels (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    label text NOT NULL CHECK (label ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    excluded boolean NOT NULL DEFAULT false,
    PRIMARY KEY (job_id, label, excluded)
);
