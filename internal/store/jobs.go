package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
)

type Job struct {
	ID, TenantID, Name, Description, ScheduleType, ScheduleExpression, Timezone                     string
	TargetURL, HTTPMethod, BodyTemplate, OverlapPolicy, MisfirePolicy                               string
	ExecutorGroupID, ExecutorHandler, KubernetesClusterID                                           string
	ScriptLanguage, ScriptSource                                                                    string
	RequiredExecutorLabels, ExcludedExecutorLabels                                                  []string
	Headers                                                                                         map[string]string
	DockerRegistryAuth                                                                              DockerRegistryAuth
	TimeoutSeconds, MaxRetries, MaxConcurrentRuns, MaxCatchUp, CallbackTimeoutSeconds, MaxQueueSize int32
	Enabled                                                                                         bool
	NextRunAt                                                                                       *time.Time
	Version                                                                                         int64
}

type DockerRegistryAuth struct {
	Server, Username, Password string
	Configured                 bool
}

func (s *Store) CreateJob(ctx context.Context, j Job) (Job, error) {
	j.OverlapPolicy = canonicalBlockPolicy(j.OverlapPolicy)
	if j.MaxConcurrentRuns < 1 {
		j.MaxConcurrentRuns = 1
	}
	if j.MaxCatchUp < 1 {
		j.MaxCatchUp = 10
	}
	if j.CallbackTimeoutSeconds < 1 {
		j.CallbackTimeoutSeconds = 3600
	}
	if j.MaxQueueSize < 1 {
		j.MaxQueueSize = 1000
	}
	next, err := schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, time.Now().UTC())
	if err != nil {
		return Job{}, err
	}
	if j.ID == "" {
		j.ID = uuid.NewString()
	}
	headers, err := json.Marshal(j.Headers)
	if err != nil {
		return Job{}, fmt.Errorf("marshal headers: %w", err)
	}
	var nextArg any
	if !next.IsZero() && j.Enabled {
		nextArg = next
	}
	var encrypted []byte
	var keyVersion *int
	if s.headerCipher != nil {
		ciphertext, version, encryptErr := s.headerCipher.Encrypt(headers)
		if encryptErr != nil {
			return Job{}, fmt.Errorf("encrypt headers: %w", encryptErr)
		}
		encrypted = ciphertext
		keyVersion = &version
		headers = []byte(`{}`)
	}
	dockerAuth, dockerAuthKeyVersion, err := s.encryptDockerRegistryAuth(j.DockerRegistryAuth)
	if err != nil {
		return Job{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin create job: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = tx.QueryRow(ctx, `INSERT INTO jobs
	 (id,tenant_id,name,description,schedule_type,schedule_expression,timezone,target_url,http_method,headers,encrypted_headers,encryption_key_version,encrypted_docker_registry_auth,docker_registry_auth_key_version,body_template,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled,next_run_at,max_concurrent_runs,max_catch_up,callback_timeout_seconds,max_queue_size,executor_group_id,executor_handler,script_language,script_source,kubernetes_cluster_id)
	 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,NULLIF($26,'')::uuid,$27,$28,$29,NULLIF($30,'')::uuid)
	 RETURNING next_run_at,version`, j.ID, j.TenantID, j.Name, j.Description, j.ScheduleType, j.ScheduleExpression, j.Timezone, j.TargetURL, j.HTTPMethod, headers, encrypted, keyVersion, dockerAuth, dockerAuthKeyVersion, j.BodyTemplate, j.TimeoutSeconds, j.MaxRetries, j.OverlapPolicy, j.MisfirePolicy, j.Enabled, nextArg, j.MaxConcurrentRuns, j.MaxCatchUp, j.CallbackTimeoutSeconds, j.MaxQueueSize, j.ExecutorGroupID, j.ExecutorHandler, j.ScriptLanguage, j.ScriptSource, j.KubernetesClusterID).Scan(&j.NextRunAt, &j.Version)
	if err != nil {
		return Job{}, fmt.Errorf("insert job: %w", err)
	}
	if j.ScriptLanguage != "" {
		if _, err = insertJobScriptVersion(ctx, tx, j, "initial version"); err != nil {
			return Job{}, err
		}
	}
	if err = replaceJobExecutorLabels(ctx, tx, j.ID, j.RequiredExecutorLabels, j.ExcludedExecutorLabels); err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit create job: %w", err)
	}
	return j, nil
}

const jobColumns = `id,tenant_id,name,description,schedule_type,schedule_expression,timezone,target_url,http_method,headers,encrypted_headers,encryption_key_version,encrypted_docker_registry_auth,docker_registry_auth_key_version,body_template,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled,next_run_at,version,max_concurrent_runs,max_catch_up,callback_timeout_seconds,max_queue_size,COALESCE(executor_group_id::text,''),executor_handler,script_language,script_source,COALESCE(kubernetes_cluster_id::text,'')`

// jobEnqueueColumns matches scanJob but does not load script_source.
const jobEnqueueColumns = `id,tenant_id,name,description,schedule_type,schedule_expression,timezone,target_url,http_method,headers,encrypted_headers,encryption_key_version,encrypted_docker_registry_auth,docker_registry_auth_key_version,body_template,timeout_seconds,max_retries,overlap_policy,misfire_policy,enabled,next_run_at,version,max_concurrent_runs,max_catch_up,callback_timeout_seconds,max_queue_size,COALESCE(executor_group_id::text,''),executor_handler,script_language,''::text,COALESCE(kubernetes_cluster_id::text,'')`

func (s *Store) scanJob(row pgx.Row) (Job, error) {
	var j Job
	var headers, encrypted, dockerAuth []byte
	var keyVersion, dockerAuthKeyVersion *int
	err := row.Scan(&j.ID, &j.TenantID, &j.Name, &j.Description, &j.ScheduleType, &j.ScheduleExpression, &j.Timezone, &j.TargetURL, &j.HTTPMethod, &headers, &encrypted, &keyVersion, &dockerAuth, &dockerAuthKeyVersion, &j.BodyTemplate, &j.TimeoutSeconds, &j.MaxRetries, &j.OverlapPolicy, &j.MisfirePolicy, &j.Enabled, &j.NextRunAt, &j.Version, &j.MaxConcurrentRuns, &j.MaxCatchUp, &j.CallbackTimeoutSeconds, &j.MaxQueueSize, &j.ExecutorGroupID, &j.ExecutorHandler, &j.ScriptLanguage, &j.ScriptSource, &j.KubernetesClusterID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	if len(encrypted) > 0 {
		if s.headerCipher == nil || keyVersion == nil {
			return Job{}, fmt.Errorf("encrypted headers require store cipher")
		}
		headers, err = s.headerCipher.Decrypt(encrypted, *keyVersion)
		if err != nil {
			return Job{}, fmt.Errorf("decrypt headers: %w", err)
		}
	}
	if err := json.Unmarshal(headers, &j.Headers); err != nil {
		return Job{}, fmt.Errorf("unmarshal headers: %w", err)
	}
	if err = s.decryptDockerRegistryAuth(dockerAuth, dockerAuthKeyVersion, &j.DockerRegistryAuth); err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *Store) encryptDockerRegistryAuth(auth DockerRegistryAuth) ([]byte, *int, error) {
	if !auth.Configured {
		return nil, nil, nil
	}
	if s.headerCipher == nil {
		return nil, nil, errors.New("docker registry credentials require store cipher")
	}
	if auth.Server == "" || auth.Username == "" || auth.Password == "" {
		return nil, nil, errors.New("docker registry server, username and password are required")
	}
	plain, err := json.Marshal(auth)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal docker registry credentials: %w", err)
	}
	encrypted, version, err := s.headerCipher.Encrypt(plain)
	if err != nil {
		return nil, nil, fmt.Errorf("encrypt docker registry credentials: %w", err)
	}
	return encrypted, &version, nil
}

func (s *Store) decryptDockerRegistryAuth(encrypted []byte, keyVersion *int, auth *DockerRegistryAuth) error {
	if len(encrypted) == 0 {
		return nil
	}
	if s.headerCipher == nil || keyVersion == nil {
		return errors.New("encrypted docker registry credentials require store cipher")
	}
	plain, err := s.headerCipher.Decrypt(encrypted, *keyVersion)
	if err != nil {
		return fmt.Errorf("decrypt docker registry credentials: %w", err)
	}
	if err = json.Unmarshal(plain, auth); err != nil {
		return fmt.Errorf("unmarshal docker registry credentials: %w", err)
	}
	auth.Configured = true
	return nil
}

func (s *Store) GetJob(ctx context.Context, tenantID, id string) (Job, error) {
	return s.scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND id=$2`, tenantID, id))
}

func (s *Store) ListJobs(ctx context.Context, tenantID string, limit int) ([]Job, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]Job, 0, limit)
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = s.attachJobExecutorLabels(ctx, jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) UpdateJob(ctx context.Context, j Job) (Job, error) {
	j.OverlapPolicy = canonicalBlockPolicy(j.OverlapPolicy)
	next, err := schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, time.Now().UTC())
	if err != nil {
		return Job{}, err
	}
	var nextArg any
	if j.Enabled && !next.IsZero() {
		nextArg = next
	}
	headers, marshalErr := json.Marshal(j.Headers)
	if marshalErr != nil {
		return Job{}, fmt.Errorf("marshal headers: %w", marshalErr)
	}
	var encrypted []byte
	var keyVersion *int
	if s.headerCipher != nil {
		ciphertext, version, encryptErr := s.headerCipher.Encrypt(headers)
		if encryptErr != nil {
			return Job{}, fmt.Errorf("encrypt headers: %w", encryptErr)
		}
		encrypted = ciphertext
		keyVersion = &version
		headers = []byte(`{}`)
	}
	dockerAuth, dockerAuthKeyVersion, err := s.encryptDockerRegistryAuth(j.DockerRegistryAuth)
	if err != nil {
		return Job{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin update job: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := s.scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, j.TenantID, j.ID))
	if errors.Is(err, ErrNotFound) {
		return Job{}, ErrConflict
	}
	if err != nil {
		return Job{}, err
	}
	if current.Version != j.Version {
		return Job{}, ErrConflict
	}
	scriptChanged := current.ScriptLanguage != j.ScriptLanguage || current.ScriptSource != j.ScriptSource
	err = tx.QueryRow(ctx, `UPDATE jobs SET name=$3,description=$4,schedule_type=$5,schedule_expression=$6,timezone=$7,target_url=$8,http_method=$9,headers=$10,encrypted_headers=$11,encryption_key_version=$12,encrypted_docker_registry_auth=$13,docker_registry_auth_key_version=$14,body_template=$15,timeout_seconds=$16,max_retries=$17,overlap_policy=$18,misfire_policy=$19,enabled=$20,next_run_at=$21,max_concurrent_runs=$22,max_catch_up=$23,callback_timeout_seconds=$24,max_queue_size=$25,executor_group_id=NULLIF($26,'')::uuid,executor_handler=$27,script_language=$28,script_source=$29,kubernetes_cluster_id=NULLIF($30,'')::uuid,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 RETURNING next_run_at,version`, j.TenantID, j.ID, j.Name, j.Description, j.ScheduleType, j.ScheduleExpression, j.Timezone, j.TargetURL, j.HTTPMethod, headers, encrypted, keyVersion, dockerAuth, dockerAuthKeyVersion, j.BodyTemplate, j.TimeoutSeconds, j.MaxRetries, j.OverlapPolicy, j.MisfirePolicy, j.Enabled, nextArg, j.MaxConcurrentRuns, j.MaxCatchUp, j.CallbackTimeoutSeconds, j.MaxQueueSize, j.ExecutorGroupID, j.ExecutorHandler, j.ScriptLanguage, j.ScriptSource, j.KubernetesClusterID).Scan(&j.NextRunAt, &j.Version)
	if err != nil {
		return Job{}, fmt.Errorf("update job: %w", err)
	}
	if scriptChanged && j.ScriptLanguage != "" {
		if _, err = insertJobScriptVersion(ctx, tx, j, "job update"); err != nil {
			return Job{}, err
		}
	}
	if err = replaceJobExecutorLabels(ctx, tx, j.ID, j.RequiredExecutorLabels, j.ExcludedExecutorLabels); err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit update job: %w", err)
	}
	return j, nil
}

func nextRunForEnabledState(j Job, enabled bool, now time.Time) (*time.Time, error) {
	if !enabled {
		return nil, nil
	}
	next, err := schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, now)
	if err != nil {
		return nil, err
	}
	if next.IsZero() {
		return nil, nil
	}
	return &next, nil
}

// SetJobEnabled atomically starts or stops a job without replacing its configuration.
func (s *Store) SetJobEnabled(ctx context.Context, tenantID, id string, enabled bool, version int64) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin job lifecycle update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, err := s.scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, id))
	if err != nil {
		return Job{}, err
	}
	if job.Version != version {
		return Job{}, ErrConflict
	}
	if job.Enabled == enabled {
		if err = tx.Commit(ctx); err != nil {
			return Job{}, fmt.Errorf("commit unchanged job lifecycle: %w", err)
		}
		return job, nil
	}

	nextRunAt, err := nextRunForEnabledState(job, enabled, time.Now().UTC())
	if err != nil {
		return Job{}, err
	}
	job, err = s.scanJob(tx.QueryRow(ctx, `UPDATE jobs SET enabled=$3,next_run_at=$4,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 RETURNING `+jobColumns, tenantID, id, enabled, nextRunAt))
	if err != nil {
		return Job{}, fmt.Errorf("update job lifecycle: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit job lifecycle update: %w", err)
	}
	return job, nil
}

func (s *Store) DeleteJob(ctx context.Context, tenantID, id string, version int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jobs WHERE tenant_id=$1 AND id=$2 AND version=$3`, tenantID, id, version)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}
