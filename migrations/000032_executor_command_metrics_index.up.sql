UPDATE executor_commands AS command
SET payload = command.payload || jsonb_build_object(
    'external_execution_id', run.external_execution_id,
    'job_id', run.job_id,
    'script_language', job.script_language,
    'kubernetes_cluster_id', COALESCE(job.kubernetes_cluster_id::text, '')
)
FROM job_runs AS run
JOIN jobs AS job ON job.tenant_id = run.tenant_id AND job.id = run.job_id
WHERE command.tenant_id = run.tenant_id
  AND command.run_id = run.id
  AND command.status = 'pending';

CREATE INDEX executor_commands_pending_created_idx
    ON executor_commands(created_at)
    WHERE status = 'pending';
