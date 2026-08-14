ALTER TABLE kubernetes_clusters
    ADD COLUMN max_concurrent_jobs integer NOT NULL DEFAULT 100
    CHECK (max_concurrent_jobs BETWEEN 1 AND 1000000);
