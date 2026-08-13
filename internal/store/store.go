package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("version conflict")
var ErrQueueFull = errors.New("job queue is full")
var ErrNotCancellable = errors.New("run is not cancellable")
var ErrDependencyCycle = errors.New("job dependency would create a cycle")
var ErrRegistrationMode = errors.New("executor group does not accept dynamic registration")
var ErrExecutorGroupInUse = errors.New("executor group is referenced by a job")
var ErrOverrideRequiresExecutorGroup = errors.New("executor address override requires an executor group job")
var ErrNotificationLeaseLost = errors.New("notification delivery lease lost")
var ErrNotificationConfigUnreadable = errors.New("notification channel configuration is unreadable")
var ErrInvalidNotificationScope = errors.New("notification channel must target all jobs or one or more specific jobs")

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

type Run struct {
	ID, TenantID, JobID, TriggerType, Status, RuntimeInput, ParentRunID, RetryOfRunID string
	ExternalExecutionID                                                               string
	ExecutorNodeID, ExecutorAddress                                                   string
	LeaseToken                                                                        string
	BroadcastGroupID                                                                  string
	ShardIndex, ShardTotal                                                            int32
	RescheduleOnTerminal                                                              bool
	OverrideAddresses                                                                 []string
	Attempt                                                                           int32
	ScheduledAt                                                                       time.Time
	StartedAt, FinishedAt                                                             *time.Time
	ResponseStatus                                                                    int32
	ErrorMessage                                                                      string
}

func requireRunLease(run Run) error {
	if run.ID == "" || run.LeaseToken == "" {
		return ErrConflict
	}
	return nil
}

func (s *Store) SetJobDependencies(ctx context.Context, tenantID, parentJobID string, childJobIDs []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dependency update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, tenantID, append([]string{parentJobID}, childJobIDs...)).Scan(&count); err != nil {
		return err
	}
	if count != len(childJobIDs)+1 {
		return ErrNotFound
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_dependencies WHERE tenant_id=$1 AND parent_job_id=$2`, tenantID, parentJobID); err != nil {
		return err
	}
	for _, childID := range childJobIDs {
		if childID == parentJobID {
			return ErrDependencyCycle
		}
		var cycle bool
		if err = tx.QueryRow(ctx, `WITH RECURSIVE descendants(id) AS (SELECT child_job_id FROM job_dependencies WHERE parent_job_id=$1 UNION SELECT d.child_job_id FROM job_dependencies d JOIN descendants x ON d.parent_job_id=x.id) SELECT EXISTS(SELECT 1 FROM descendants WHERE id=$2)`, childID, parentJobID).Scan(&cycle); err != nil {
			return err
		}
		if cycle {
			return ErrDependencyCycle
		}
		if _, err = tx.Exec(ctx, `INSERT INTO job_dependencies(tenant_id,parent_job_id,child_job_id) VALUES($1,$2,$3)`, tenantID, parentJobID, childID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) JobDependencies(ctx context.Context, tenantID, parentJobID string) ([]string, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE tenant_id=$1 AND id=$2)`, tenantID, parentJobID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT child_job_id FROM job_dependencies WHERE tenant_id=$1 AND parent_job_id=$2 ORDER BY child_job_id`, tenantID, parentJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type Store struct {
	pool         *pgxpool.Pool
	headerCipher HeaderCipher
}

type PoolStats struct {
	AcquiredConnections int32
	IdleConnections     int32
	TotalConnections    int32
	MaxConnections      int32
	EmptyAcquireCount   int64
	AcquireDuration     time.Duration
}
type HeaderCipher interface {
	Encrypt([]byte) ([]byte, int, error)
	Decrypt([]byte, int) ([]byte, error)
}
type storeOptions struct {
	headerCipher HeaderCipher
	maxConns     int32
	minConns     int32
}

type Option func(*storeOptions)

type blockAction string

const (
	blockEnqueue          blockAction = "enqueue"
	blockSkip             blockAction = "skip"
	blockCancelAndEnqueue blockAction = "cancel_and_enqueue"
)

func canonicalBlockPolicy(policy string) string {
	switch policy {
	case "queue":
		return "serial"
	case "skip":
		return "discard_later"
	default:
		return policy
	}
}

func decideBlockAction(policy string, hasActive bool) blockAction {
	if !hasActive {
		return blockEnqueue
	}
	switch canonicalBlockPolicy(policy) {
	case "discard_later":
		return blockSkip
	case "cover_early":
		return blockCancelAndEnqueue
	default:
		return blockEnqueue
	}
}

func WithHeaderCipher(cipher HeaderCipher) Option {
	return func(options *storeOptions) { options.headerCipher = cipher }
}

func WithPoolSize(maxConns, minConns int32) Option {
	return func(options *storeOptions) {
		options.maxConns = maxConns
		options.minConns = minConns
	}
}

func New(ctx context.Context, databaseURL string, opts ...Option) (*Store, error) {
	options := storeOptions{maxConns: 32, minConns: 2}
	for _, opt := range opts {
		opt(&options)
	}
	if options.maxConns < 1 || options.minConns < 0 || options.minConns > options.maxConns {
		return nil, fmt.Errorf("invalid database pool size: min=%d max=%d", options.minConns, options.maxConns)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	config.MaxConns = options.maxConns
	config.MinConns = options.minConns
	config.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool, headerCipher: options.headerCipher}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) PoolStats() PoolStats {
	stats := s.pool.Stat()
	return PoolStats{
		AcquiredConnections: stats.AcquiredConns(),
		IdleConnections:     stats.IdleConns(),
		TotalConnections:    stats.TotalConns(),
		MaxConnections:      stats.MaxConns(),
		EmptyAcquireCount:   stats.EmptyAcquireCount(),
		AcquireDuration:     stats.AcquireDuration(),
	}
}
func (s *Store) AuthenticateAPIKey(ctx context.Context, raw string) (string, string, error) {
	hash := sha256.Sum256([]byte(raw))
	var tenantID, role string
	err := s.pool.QueryRow(ctx, `SELECT tenant_id,role FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL`, hash[:]).Scan(&tenantID, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", fmt.Errorf("authenticate api key: %w", err)
	}
	return tenantID, role, nil
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
	return jobs, rows.Err()
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

func (s *Store) TriggerJob(ctx context.Context, tenantID, jobID, key, input string) (Run, error) {
	return s.TriggerJobWithOptions(ctx, tenantID, jobID, key, input, TriggerOptions{})
}

type TriggerOptions struct {
	OverrideAddresses []string
}

func (s *Store) TriggerJobWithOptions(ctx context.Context, tenantID, jobID, key, input string, options TriggerOptions) (Run, error) {
	if options.OverrideAddresses == nil {
		options.OverrideAddresses = []string{}
	}
	r := Run{ID: uuid.NewString(), TenantID: tenantID, JobID: jobID, TriggerType: "manual", Status: "pending", Attempt: 1, ScheduledAt: time.Now().UTC(), RuntimeInput: input, OverrideAddresses: options.OverrideAddresses}
	r.ExternalExecutionID = r.ID
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("begin trigger: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, jobID); err != nil {
		return Run{}, fmt.Errorf("lock job queue: %w", err)
	}
	var maxQueue, pending int
	var blockPolicy, routeStrategy, executorGroupID string
	err = tx.QueryRow(ctx, `SELECT j.max_queue_size,j.overlap_policy,COALESCE(g.route_strategy,''),COALESCE(j.executor_group_id::text,''),(SELECT count(*) FROM job_runs WHERE job_id=$1 AND status='pending') FROM jobs j LEFT JOIN executor_groups g ON g.id=j.executor_group_id WHERE j.id=$1 AND j.tenant_id=$2`, jobID, tenantID).Scan(&maxQueue, &blockPolicy, &routeStrategy, &executorGroupID, &pending)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("check job queue: %w", err)
	}
	if len(options.OverrideAddresses) > 0 && executorGroupID == "" {
		return Run{}, ErrOverrideRequiresExecutorGroup
	}
	if key != "" {
		var inserted string
		err = tx.QueryRow(ctx, `INSERT INTO job_run_idempotency(tenant_id,job_id,idempotency_key,run_id,scheduled_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING run_id`, tenantID, jobID, key, r.ID, r.ScheduledAt).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			r, err = scanRun(tx.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE (id,scheduled_at)=(SELECT run_id,scheduled_at FROM job_run_idempotency WHERE tenant_id=$1 AND job_id=$2 AND idempotency_key=$3)`, tenantID, jobID, key))
			if err != nil {
				return Run{}, fmt.Errorf("read idempotent run: %w", err)
			}
			if err = tx.Commit(ctx); err != nil {
				return Run{}, err
			}
			return r, nil
		}
		if err != nil {
			return Run{}, fmt.Errorf("reserve idempotency key: %w", err)
		}
	}
	action, err := applyBlockPolicy(ctx, tx, jobID, blockPolicy)
	if err != nil {
		return Run{}, err
	}
	if action == blockEnqueue && pending >= maxQueue {
		return Run{}, ErrQueueFull
	}
	status := "pending"
	if action == blockSkip {
		status = "skipped"
	}
	if routeStrategy == "sharding_broadcast" && status == "pending" {
		var nodes []ExecutorNode
		if len(options.OverrideAddresses) > 0 {
			nodes = OverrideExecutorNodes(options.OverrideAddresses)
		} else {
			var nodesErr error
			nodes, nodesErr = liveExecutorNodesTx(ctx, tx, executorGroupID)
			if nodesErr != nil {
				return Run{}, nodesErr
			}
			required, excluded, labelsErr := jobExecutorLabels(ctx, tx, jobID)
			if labelsErr != nil {
				return Run{}, labelsErr
			}
			nodes = FilterExecutorNodes(nodes, required, excluded)
		}
		shards := planBroadcastShards(nodes)
		if len(shards) == 0 {
			return Run{}, fmt.Errorf("no live executor nodes")
		}
		if action == blockEnqueue && pending+len(shards) > maxQueue {
			return Run{}, ErrQueueFull
		}
		r.BroadcastGroupID = uuid.NewString()
		for index, shard := range shards {
			runID := uuid.NewString()
			idempotencyKey := ""
			if index == 0 {
				runID = r.ID
				idempotencyKey = key
			}
			_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,runtime_input,idempotency_key,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,override_addresses,external_execution_id) VALUES($1,$2,$3,'manual','pending',$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,$12,$1)`, runID, tenantID, jobID, r.ScheduledAt, input, idempotencyKey, shard.NodeID, shard.Address, r.BroadcastGroupID, shard.Index, shard.Total, options.OverrideAddresses)
			if err != nil {
				return Run{}, fmt.Errorf("insert broadcast shard: %w", err)
			}
			if err = emitRunLifecycleEventTx(ctx, tx, runID, "pending"); err != nil {
				return Run{}, err
			}
		}
		r.ShardIndex = 0
		r.ShardTotal = int32(len(shards)) // #nosec G115 -- executor addresses are validated to at most 100 entries.
		r.ExecutorNodeID = shards[0].NodeID
		r.ExecutorAddress = shards[0].Address
	} else {
		err = tx.QueryRow(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,runtime_input,idempotency_key,error_message,override_addresses,external_execution_id) VALUES($1,$2,$3,'manual',$4,$5,CASE WHEN $4='skipped' THEN now() END,$6,NULLIF($7,''),CASE WHEN $4='skipped' THEN 'block strategy: discard later' ELSE NULL END,$8,$1) RETURNING id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,runtime_input`, r.ID, tenantID, jobID, status, r.ScheduledAt, input, key, options.OverrideAddresses).Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.RuntimeInput)
		if err != nil {
			return Run{}, fmt.Errorf("insert run: %w", err)
		}
		if err = emitRunLifecycleEventTx(ctx, tx, r.ID, status); err != nil {
			return Run{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit trigger: %w", err)
	}
	return r, nil
}

func applyBlockPolicy(ctx context.Context, tx pgx.Tx, jobID, policy string) (blockAction, error) {
	var hasActive bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE job_id=$1 AND status IN ('pending','running','waiting_callback'))`, jobID).Scan(&hasActive); err != nil {
		return "", fmt.Errorf("check active job runs: %w", err)
	}
	action := decideBlockAction(policy, hasActive)
	return applyBlockAction(ctx, tx, jobID, action)
}

func applyBlockAction(ctx context.Context, tx pgx.Tx, jobID string, action blockAction) (blockAction, error) {
	if action == blockCancelAndEnqueue {
		rows, err := tx.Query(ctx, `UPDATE job_runs SET status='cancelled',finished_at=now(),error_message='block strategy: covered by newer trigger',lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE job_id=$1 AND status IN ('pending','running','waiting_callback') RETURNING id,tenant_id,job_id,COALESCE(broadcast_group_id::text,''),reschedule_on_terminal,finished_at`, jobID)
		if err != nil {
			return "", fmt.Errorf("cancel covered job runs: %w", err)
		}
		type coveredRun struct {
			run        Run
			finishedAt time.Time
		}
		var covered []coveredRun
		for rows.Next() {
			var item coveredRun
			if err = rows.Scan(&item.run.ID, &item.run.TenantID, &item.run.JobID, &item.run.BroadcastGroupID, &item.run.RescheduleOnTerminal, &item.finishedAt); err != nil {
				rows.Close()
				return "", err
			}
			covered = append(covered, item)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return "", err
		}
		for _, item := range covered {
			if err = emitRunLifecycleEventTx(ctx, tx, item.run.ID, "cancelled"); err != nil {
				return "", err
			}
			if err = rearmFixedDelay(ctx, tx, item.run, item.finishedAt); err != nil {
				return "", err
			}
		}
	}
	return action, nil
}

func jobRunQueueState(ctx context.Context, tx pgx.Tx, jobID string) (int, bool, error) {
	var queued int
	var hasActive bool
	err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='pending'),count(*) FILTER (WHERE status IN ('pending','running','waiting_callback'))>0 FROM job_runs WHERE job_id=$1`, jobID).Scan(&queued, &hasActive)
	if err != nil {
		return 0, false, fmt.Errorf("query job run queue state: %w", err)
	}
	return queued, hasActive, nil
}

func executorRouteStrategies(ctx context.Context, tx pgx.Tx, jobs []Job) (map[string]string, error) {
	groupIDs := make([]string, 0, len(jobs))
	seen := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		if job.ExecutorGroupID == "" {
			continue
		}
		if _, ok := seen[job.ExecutorGroupID]; ok {
			continue
		}
		seen[job.ExecutorGroupID] = struct{}{}
		groupIDs = append(groupIDs, job.ExecutorGroupID)
	}
	strategies := make(map[string]string, len(groupIDs))
	if len(groupIDs) == 0 {
		return strategies, nil
	}
	rows, err := tx.Query(ctx, `SELECT id::text,route_strategy FROM executor_groups WHERE id=ANY($1::uuid[])`, groupIDs)
	if err != nil {
		return nil, fmt.Errorf("query executor route strategies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, strategy string
		if err = rows.Scan(&id, &strategy); err != nil {
			return nil, err
		}
		strategies[id] = strategy
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(strategies) != len(groupIDs) {
		return nil, ErrNotFound
	}
	return strategies, nil
}

func (s *Store) EnqueueDue(ctx context.Context, batch int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE enabled AND next_run_at<=now() ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT $1`, batch)
	if err != nil {
		return err
	}
	var jobs []Job
	for rows.Next() {
		j, e := s.scanJob(rows)
		if e != nil {
			rows.Close()
			return e
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	routeStrategies, err := executorRouteStrategies(ctx, tx, jobs)
	if err != nil {
		return err
	}
	var fastBatch pgx.Batch
	for _, j := range jobs {
		if j.NextRunAt == nil {
			continue
		}
		now := time.Now().UTC()
		due, next, e := schedule.Due(j.ScheduleType, j.ScheduleExpression, j.Timezone, j.MisfirePolicy, *j.NextRunAt, now, int(j.MaxCatchUp))
		if e != nil {
			return e
		}
		routeStrategy := routeStrategies[j.ExecutorGroupID]
		fastPath := j.OverlapPolicy == "parallel" && routeStrategy != "sharding_broadcast"
		for _, scheduledAt := range due {
			if fastPath {
				runID := uuid.NewString()
				fastBatch.Queue(`INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule','pending',$4,$5,$1)`, runID, j.TenantID, j.ID, scheduledAt, j.ScheduleType == "fixed_delay")
				fastBatch.Queue(runLifecycleEventSQL, runID, "pending", "pending", uuid.NewString())
				continue
			}
			queued, hasActive, stateErr := jobRunQueueState(ctx, tx, j.ID)
			if stateErr != nil {
				return stateErr
			}
			action, actionErr := applyBlockAction(ctx, tx, j.ID, decideBlockAction(j.OverlapPolicy, hasActive))
			if actionErr != nil {
				return actionErr
			}
			status := "pending"
			if action == blockSkip || (action == blockEnqueue && queued >= int(j.MaxQueueSize)) {
				status = "skipped"
			}
			if routeStrategy == "sharding_broadcast" && status == "pending" {
				nodes, nodesErr := liveExecutorNodesTx(ctx, tx, j.ExecutorGroupID)
				if nodesErr != nil {
					return nodesErr
				}
				required, excluded, labelsErr := jobExecutorLabels(ctx, tx, j.ID)
				if labelsErr != nil {
					return labelsErr
				}
				nodes = FilterExecutorNodes(nodes, required, excluded)
				shards := planBroadcastShards(nodes)
				if len(shards) == 0 {
					return fmt.Errorf("no live executor nodes for broadcast job %s", j.ID)
				}
				if action == blockEnqueue && queued+len(shards) > int(j.MaxQueueSize) {
					status = "skipped"
				} else {
					groupID := uuid.NewString()
					for _, shard := range shards {
						runID := uuid.NewString()
						if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule','pending',$4,$5,$6,$7,$8,$9,$10,$1)`, runID, j.TenantID, j.ID, scheduledAt, shard.NodeID, shard.Address, groupID, shard.Index, shard.Total, j.ScheduleType == "fixed_delay"); err != nil {
							return err
						}
						if err = emitRunLifecycleEventTx(ctx, tx, runID, "pending"); err != nil {
							return err
						}
					}
					continue
				}
			}
			runID := uuid.NewString()
			_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,error_message,reschedule_on_terminal,external_execution_id) VALUES($1,$2,$3,'schedule',$4,$5,CASE WHEN $4='skipped' THEN now() END,CASE WHEN $4='skipped' THEN 'block strategy: discard later or queue full' ELSE NULL END,$6,$1)`, runID, j.TenantID, j.ID, status, scheduledAt, j.ScheduleType == "fixed_delay")
			if err != nil {
				return err
			}
			if err = emitRunLifecycleEventTx(ctx, tx, runID, status); err != nil {
				return err
			}
			if j.ScheduleType == "fixed_delay" && status == "skipped" {
				next, err = schedule.Next(j.ScheduleType, j.ScheduleExpression, j.Timezone, now)
				if err != nil {
					return err
				}
			}
		}
		var arg any
		if !next.IsZero() {
			arg = next
		}
		if fastPath {
			fastBatch.Queue(`UPDATE jobs SET last_run_at=next_run_at,next_run_at=$2,enabled=CASE WHEN $2::timestamptz IS NULL AND schedule_type='once' THEN false ELSE enabled END,updated_at=now() WHERE id=$1`, j.ID, arg)
		} else {
			_, err = tx.Exec(ctx, `UPDATE jobs SET last_run_at=next_run_at,next_run_at=$2,enabled=CASE WHEN $2::timestamptz IS NULL AND schedule_type='once' THEN false ELSE enabled END,updated_at=now() WHERE id=$1`, j.ID, arg)
			if err != nil {
				return err
			}
		}
	}
	if fastBatch.Len() > 0 {
		results := tx.SendBatch(ctx, &fastBatch)
		for index := 0; index < fastBatch.Len(); index++ {
			if _, err = results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("execute due-run batch: %w", err)
			}
		}
		if err = results.Close(); err != nil {
			return fmt.Errorf("close due-run batch: %w", err)
		}
	}
	return tx.Commit(ctx)
}

type ClaimedRun struct {
	Run Run
	Job Job
}

func (s *Store) ClaimRuns(ctx context.Context, owner string, limit int, lease time.Duration) ([]ClaimedRun, error) {
	rows, err := s.pool.Query(ctx, `WITH active AS MATERIALIZED (
	 SELECT job_id,tenant_id FROM job_runs WHERE (status='running' AND lease_until>=now()) OR status='waiting_callback'
	), active_job AS (
	 SELECT job_id,count(*) AS n FROM active GROUP BY job_id
	), active_tenant AS (
	 SELECT tenant_id,count(*) AS n FROM active GROUP BY tenant_id
	), candidates AS (
	 SELECT r.id,r.job_id,r.tenant_id,
	 row_number() OVER(PARTITION BY r.job_id ORDER BY r.available_at,r.id) AS job_rank,
	 row_number() OVER(PARTITION BY r.tenant_id ORDER BY r.available_at,r.id) AS tenant_rank,
		 CASE WHEN r.broadcast_group_id IS NOT NULL THEN GREATEST(j.max_concurrent_runs,r.shard_total) ELSE j.max_concurrent_runs END AS max_concurrent_runs,t.max_concurrent_runs AS tenant_max,j.timeout_seconds,
	 COALESCE(aj.n,0) AS job_active,COALESCE(at.n,0) AS tenant_active
	 FROM job_runs r JOIN jobs j ON j.id=r.job_id JOIN tenants t ON t.id=r.tenant_id
	 LEFT JOIN active_job aj ON aj.job_id=r.job_id LEFT JOIN active_tenant at ON at.tenant_id=r.tenant_id
	 WHERE (r.status='pending' AND r.available_at<=now()) OR (r.status='running' AND r.lease_until<now())
	), eligible AS (
	 SELECT r.id,c.timeout_seconds,r.status='pending' AS emit_running FROM job_runs r JOIN candidates c ON c.id=r.id
	 WHERE c.job_rank<=GREATEST(c.max_concurrent_runs-c.job_active,0)
	 AND c.tenant_rank<=GREATEST(c.tenant_max-c.tenant_active,0)
	 ORDER BY r.available_at,r.id FOR UPDATE OF r SKIP LOCKED LIMIT $1
	), claimed AS (UPDATE job_runs r SET status='running',lease_owner=$2,lease_token=gen_random_uuid(),lease_until=now()+GREATEST($3,eligible.timeout_seconds+30)*interval '1 second',started_at=COALESCE(started_at,now()) FROM eligible WHERE r.id=eligible.id AND ((r.status='pending' AND r.available_at<=now()) OR (r.status='running' AND r.lease_until<now())) RETURNING r.id,r.tenant_id,r.job_id,r.trigger_type,r.status,r.attempt,r.scheduled_at,r.runtime_input,r.parent_run_id,r.retry_of_run_id,r.external_execution_id,r.executor_node_id,r.executor_address,r.broadcast_group_id,r.shard_index,r.shard_total,r.reschedule_on_terminal,r.override_addresses,r.lease_token,eligible.emit_running
	), emitted AS (INSERT INTO outbox_events(id,tenant_id,topic,payload)
	 SELECT gen_random_uuid(),c.tenant_id,'job.run.running',jsonb_build_object('run_id',c.id::text,'job_id',c.job_id::text,'tenant_id',c.tenant_id::text,'status','running','attempt',c.attempt,'trigger_type',c.trigger_type,'scheduled_at',c.scheduled_at,'occurred_at',now())
	 FROM claimed c WHERE c.emit_running)
	 SELECT c.id,c.tenant_id,c.job_id,c.trigger_type,c.status,c.attempt,c.scheduled_at,c.runtime_input,COALESCE(c.parent_run_id::text,''),COALESCE(c.retry_of_run_id::text,''),c.external_execution_id,COALESCE(c.executor_node_id,''),COALESCE(c.executor_address,''),COALESCE(c.broadcast_group_id::text,''),COALESCE(c.shard_index,0),COALESCE(c.shard_total,0),c.reschedule_on_terminal,c.override_addresses,c.lease_token,`+jobColumnsWithAlias("j")+` FROM claimed c JOIN jobs j ON j.id=c.job_id`, limit, owner, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedRun
	for rows.Next() {
		var x ClaimedRun
		var headers, encrypted, dockerAuth []byte
		var keyVersion, dockerAuthKeyVersion *int
		err = rows.Scan(&x.Run.ID, &x.Run.TenantID, &x.Run.JobID, &x.Run.TriggerType, &x.Run.Status, &x.Run.Attempt, &x.Run.ScheduledAt, &x.Run.RuntimeInput, &x.Run.ParentRunID, &x.Run.RetryOfRunID, &x.Run.ExternalExecutionID, &x.Run.ExecutorNodeID, &x.Run.ExecutorAddress, &x.Run.BroadcastGroupID, &x.Run.ShardIndex, &x.Run.ShardTotal, &x.Run.RescheduleOnTerminal, &x.Run.OverrideAddresses, &x.Run.LeaseToken, &x.Job.ID, &x.Job.TenantID, &x.Job.Name, &x.Job.Description, &x.Job.ScheduleType, &x.Job.ScheduleExpression, &x.Job.Timezone, &x.Job.TargetURL, &x.Job.HTTPMethod, &headers, &encrypted, &keyVersion, &dockerAuth, &dockerAuthKeyVersion, &x.Job.BodyTemplate, &x.Job.TimeoutSeconds, &x.Job.MaxRetries, &x.Job.OverlapPolicy, &x.Job.MisfirePolicy, &x.Job.Enabled, &x.Job.NextRunAt, &x.Job.Version, &x.Job.MaxConcurrentRuns, &x.Job.MaxCatchUp, &x.Job.CallbackTimeoutSeconds, &x.Job.MaxQueueSize, &x.Job.ExecutorGroupID, &x.Job.ExecutorHandler, &x.Job.ScriptLanguage, &x.Job.ScriptSource, &x.Job.KubernetesClusterID)
		if err != nil {
			return nil, err
		}
		if len(encrypted) > 0 {
			if s.headerCipher == nil || keyVersion == nil {
				return nil, fmt.Errorf("encrypted headers require store cipher")
			}
			headers, err = s.headerCipher.Decrypt(encrypted, *keyVersion)
			if err != nil {
				return nil, err
			}
		}
		if err = json.Unmarshal(headers, &x.Job.Headers); err != nil {
			return nil, err
		}
		if err = s.decryptDockerRegistryAuth(dockerAuth, dockerAuthKeyVersion, &x.Job.DockerRegistryAuth); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func jobColumnsWithAlias(a string) string {
	return a + `.id,` + a + `.tenant_id,` + a + `.name,` + a + `.description,` + a + `.schedule_type,` + a + `.schedule_expression,` + a + `.timezone,` + a + `.target_url,` + a + `.http_method,` + a + `.headers,` + a + `.encrypted_headers,` + a + `.encryption_key_version,` + a + `.encrypted_docker_registry_auth,` + a + `.docker_registry_auth_key_version,` + a + `.body_template,` + a + `.timeout_seconds,` + a + `.max_retries,` + a + `.overlap_policy,` + a + `.misfire_policy,` + a + `.enabled,` + a + `.next_run_at,` + a + `.version,` + a + `.max_concurrent_runs,` + a + `.max_catch_up,` + a + `.callback_timeout_seconds,` + a + `.max_queue_size,COALESCE(` + a + `.executor_group_id::text,''),` + a + `.executor_handler,` + a + `.script_language,` + a + `.script_source,COALESCE(` + a + `.kubernetes_cluster_id::text,'')`
}

func (s *Store) CompleteRun(ctx context.Context, r Run, success bool, status int, body, errorMessage string) error {
	if err := requireRunLease(r); err != nil {
		return err
	}
	state := "succeeded"
	if !success {
		state = "failed"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var finishedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE job_runs SET status=$2,response_status=$3,response_body=$4,error_message=$5,finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='running' AND lease_token=$6 RETURNING finished_at`, r.ID, state, status, body, errorMessage, r.LeaseToken).Scan(&finishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, r.ID, state); err != nil {
		return err
	}
	if !success {
		if err = emitRunEventTx(ctx, tx, r.ID, state, "exhausted"); err != nil {
			return err
		}
	}
	if success {
		if err = enqueueDependentRuns(ctx, tx, r); err != nil {
			return err
		}
	}
	if err = rearmFixedDelay(ctx, tx, r, finishedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func rearmFixedDelay(ctx context.Context, tx pgx.Tx, r Run, terminalAt time.Time) error {
	if !r.RescheduleOnTerminal {
		return nil
	}
	if r.BroadcastGroupID != "" {
		var unfinished bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_runs WHERE broadcast_group_id=$1 AND reschedule_on_terminal AND status IN ('pending','running','waiting_callback'))`, r.BroadcastGroupID).Scan(&unfinished); err != nil {
			return err
		}
		if unfinished {
			return nil
		}
	}
	var expression, timezone string
	var enabled bool
	var existingNext *time.Time
	err := tx.QueryRow(ctx, `SELECT schedule_expression,timezone,enabled,next_run_at FROM jobs WHERE id=$1 AND schedule_type='fixed_delay' FOR UPDATE`, r.JobID).Scan(&expression, &timezone, &enabled, &existingNext)
	if errors.Is(err, pgx.ErrNoRows) || !enabled || existingNext != nil {
		return nil
	}
	if err != nil {
		return err
	}
	next, err := schedule.Next("fixed_delay", expression, timezone, terminalAt)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET next_run_at=$2,last_run_at=$3,updated_at=now() WHERE id=$1 AND enabled AND schedule_type='fixed_delay' AND next_run_at IS NULL`, r.JobID, next, terminalAt)
	return err
}

func enqueueDependentRuns(ctx context.Context, tx pgx.Tx, parent Run) error {
	rows, err := tx.Query(ctx, `SELECT d.child_job_id,j.overlap_policy,j.max_queue_size,COALESCE(j.executor_group_id::text,''),COALESCE(g.route_strategy,'') FROM job_dependencies d JOIN jobs j ON j.id=d.child_job_id LEFT JOIN executor_groups g ON g.id=j.executor_group_id WHERE d.tenant_id=$1 AND d.parent_job_id=$2 ORDER BY d.child_job_id`, parent.TenantID, parent.JobID)
	if err != nil {
		return fmt.Errorf("query dependent jobs: %w", err)
	}
	type dependent struct {
		id, policy, executorGroupID, routeStrategy string
		maxQueue                                   int
	}
	var children []dependent
	for rows.Next() {
		var child dependent
		if err = rows.Scan(&child.id, &child.policy, &child.maxQueue, &child.executorGroupID, &child.routeStrategy); err != nil {
			rows.Close()
			return err
		}
		children = append(children, child)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, child := range children {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, child.id); err != nil {
			return err
		}
		runID := uuid.NewString()
		scheduledAt := time.Now().UTC()
		tag, reserveErr := tx.Exec(ctx, `INSERT INTO job_dependency_dispatches(parent_run_id,child_job_id,child_run_id,child_scheduled_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, parent.ID, child.id, runID, scheduledAt)
		if reserveErr != nil {
			return reserveErr
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		var pending int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1 AND status='pending'`, child.id).Scan(&pending); err != nil {
			return err
		}
		action, actionErr := applyBlockPolicy(ctx, tx, child.id, child.policy)
		if actionErr != nil {
			return actionErr
		}
		state := "pending"
		message := ""
		if action == blockSkip || (action == blockEnqueue && pending >= child.maxQueue) {
			state = "skipped"
			message = "dependency trigger skipped by block strategy or queue limit"
		}
		if child.routeStrategy == "sharding_broadcast" && state == "pending" {
			nodes, nodesErr := liveExecutorNodesTx(ctx, tx, child.executorGroupID)
			if nodesErr != nil {
				return nodesErr
			}
			required, excluded, labelsErr := jobExecutorLabels(ctx, tx, child.id)
			if labelsErr != nil {
				return labelsErr
			}
			nodes = FilterExecutorNodes(nodes, required, excluded)
			shards := planBroadcastShards(nodes)
			if len(shards) == 0 {
				return fmt.Errorf("no live executor nodes for dependent broadcast job %s", child.id)
			}
			if action == blockEnqueue && pending+len(shards) > child.maxQueue {
				state = "skipped"
				message = "dependency trigger skipped by queue limit"
			} else {
				groupID := uuid.NewString()
				for index, shard := range shards {
					shardRunID := uuid.NewString()
					if index == 0 {
						shardRunID = runID
					}
					if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,parent_run_id,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,external_execution_id) VALUES($1,$2,$3,'dependency','pending',$4,$5,$6,$7,$8,$9,$10,$1)`, shardRunID, parent.TenantID, child.id, scheduledAt, parent.ID, shard.NodeID, shard.Address, groupID, shard.Index, shard.Total); err != nil {
						return err
					}
					if err = emitRunLifecycleEventTx(ctx, tx, shardRunID, "pending"); err != nil {
						return err
					}
				}
				continue
			}
		}
		if _, err = tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,scheduled_at,finished_at,error_message,parent_run_id,external_execution_id) VALUES($1,$2,$3,'dependency',$4,$5,CASE WHEN $4='skipped' THEN now() END,NULLIF($6,''),$7,$1)`, runID, parent.TenantID, child.id, state, scheduledAt, message, parent.ID); err != nil {
			return err
		}
		if err = emitRunLifecycleEventTx(ctx, tx, runID, state); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkClaimedWaitingCallback(ctx context.Context, run Run, status int, tokenHash []byte, deadline time.Time) error {
	if err := requireRunLease(run); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE job_runs SET status='waiting_callback',response_status=$2,callback_token_hash=$3,callback_deadline=$4,lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND status='running' AND lease_token=$5`, run.ID, status, tokenHash, deadline, run.LeaseToken)
	if err != nil {
		return fmt.Errorf("mark waiting callback: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = emitRunLifecycleEventTx(ctx, tx, run.ID, "waiting_callback"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CompleteCallback(ctx context.Context, runID string, tokenHash []byte, succeeded bool, message string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var terminal Run
	var maxRetries int32
	err = tx.QueryRow(ctx, `WITH updated AS (
	 UPDATE job_runs SET status=CASE WHEN $3 THEN 'succeeded' ELSE 'failed' END,error_message=CASE WHEN $3 THEN '' ELSE $4 END,finished_at=now(),callback_consumed_at=now(),callback_token_hash=NULL,callback_deadline=NULL
	 WHERE id=$1 AND status='waiting_callback' AND callback_token_hash=$2 AND callback_deadline>now() RETURNING *
	) SELECT `+runColumns("u")+`,j.max_retries FROM updated u JOIN jobs j ON j.id=u.job_id`, runID, tokenHash, succeeded, message).Scan(&terminal.ID, &terminal.TenantID, &terminal.JobID, &terminal.TriggerType, &terminal.Status, &terminal.Attempt, &terminal.ScheduledAt, &terminal.StartedAt, &terminal.FinishedAt, &terminal.ResponseStatus, &terminal.ErrorMessage, &terminal.RuntimeInput, &terminal.ParentRunID, &terminal.RetryOfRunID, &terminal.ExternalExecutionID, &terminal.ExecutorNodeID, &terminal.ExecutorAddress, &terminal.BroadcastGroupID, &terminal.ShardIndex, &terminal.ShardTotal, &terminal.RescheduleOnTerminal, &terminal.OverrideAddresses, &maxRetries)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("complete callback: %w", err)
	}
	finishedAt := *terminal.FinishedAt
	if err = emitRunLifecycleEventTx(ctx, tx, runID, terminal.Status); err != nil {
		return err
	}
	willRetry := !succeeded && terminal.Attempt <= maxRetries
	if !succeeded && !willRetry {
		if err = emitRunEventTx(ctx, tx, runID, terminal.Status, "exhausted"); err != nil {
			return err
		}
	}
	if willRetry {
		if _, err = insertRetryRunTx(ctx, tx, terminal, callbackRetryDelay(terminal.Attempt)); err != nil {
			return err
		}
	} else if succeeded {
		if err = enqueueDependentRuns(ctx, tx, Run{ID: runID, TenantID: terminal.TenantID, JobID: terminal.JobID}); err != nil {
			return err
		}
	}
	if !willRetry {
		if err = rearmFixedDelay(ctx, tx, terminal, finishedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func callbackRetryDelay(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 6 {
		shift = 6
	}
	return time.Second * time.Duration(1<<shift)
}

func insertRetryRunTx(ctx context.Context, tx pgx.Tx, previous Run, delay time.Duration) (Run, error) {
	next := Run{ID: uuid.NewString(), TenantID: previous.TenantID, JobID: previous.JobID, TriggerType: "retry", Status: "pending", RuntimeInput: previous.RuntimeInput, ParentRunID: previous.ParentRunID, RetryOfRunID: previous.ID, Attempt: previous.Attempt + 1, ScheduledAt: time.Now().UTC(), ExecutorNodeID: previous.ExecutorNodeID, ExecutorAddress: previous.ExecutorAddress, BroadcastGroupID: previous.BroadcastGroupID, ShardIndex: previous.ShardIndex, ShardTotal: previous.ShardTotal, RescheduleOnTerminal: previous.RescheduleOnTerminal, OverrideAddresses: previous.OverrideAddresses}
	next.ExternalExecutionID = next.ID
	_, err := tx.Exec(ctx, `INSERT INTO job_runs(id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,available_at,runtime_input,parent_run_id,retry_of_run_id,external_execution_id,executor_node_id,executor_address,broadcast_group_id,shard_index,shard_total,reschedule_on_terminal,override_addresses) VALUES($1,$2,$3,'retry','pending',$4,$5::timestamptz,$5::timestamptz+$6*interval '1 second',$7,NULLIF($8,'')::uuid,$9,$10,NULLIF($11,''),NULLIF($12,''),NULLIF($13,'')::uuid,CASE WHEN $13='' THEN NULL ELSE $14::integer END,CASE WHEN $13='' THEN NULL ELSE $15::integer END,$16,$17)`, next.ID, next.TenantID, next.JobID, next.Attempt, next.ScheduledAt, delay.Seconds(), next.RuntimeInput, next.ParentRunID, next.RetryOfRunID, next.ExternalExecutionID, next.ExecutorNodeID, next.ExecutorAddress, next.BroadcastGroupID, next.ShardIndex, next.ShardTotal, next.RescheduleOnTerminal, next.OverrideAddresses)
	if err != nil {
		return Run{}, err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, next.ID, "pending"); err != nil {
		return Run{}, err
	}
	return next, nil
}

func runColumns(alias string) string {
	return alias + `.id,` + alias + `.tenant_id,` + alias + `.job_id,` + alias + `.trigger_type,` + alias + `.status,` + alias + `.attempt,` + alias + `.scheduled_at,` + alias + `.started_at,` + alias + `.finished_at,COALESCE(` + alias + `.response_status,0),COALESCE(` + alias + `.error_message,''),` + alias + `.runtime_input,COALESCE(` + alias + `.parent_run_id::text,''),COALESCE(` + alias + `.retry_of_run_id::text,''),` + alias + `.external_execution_id,COALESCE(` + alias + `.executor_node_id,''),COALESCE(` + alias + `.executor_address,''),COALESCE(` + alias + `.broadcast_group_id::text,''),COALESCE(` + alias + `.shard_index,0),COALESCE(` + alias + `.shard_total,0),` + alias + `.reschedule_on_terminal,` + alias + `.override_addresses`
}

func (s *Store) ExpireCallbacks(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT `+runColumns("r")+`,j.max_retries FROM job_runs r JOIN jobs j ON j.id=r.job_id WHERE r.status='waiting_callback' AND r.callback_deadline<=now() FOR UPDATE OF r SKIP LOCKED`)
	if err != nil {
		return err
	}
	type expiredRun struct {
		run        Run
		maxRetries int32
	}
	var expired []expiredRun
	for rows.Next() {
		var item expiredRun
		if err = rows.Scan(&item.run.ID, &item.run.TenantID, &item.run.JobID, &item.run.TriggerType, &item.run.Status, &item.run.Attempt, &item.run.ScheduledAt, &item.run.StartedAt, &item.run.FinishedAt, &item.run.ResponseStatus, &item.run.ErrorMessage, &item.run.RuntimeInput, &item.run.ParentRunID, &item.run.RetryOfRunID, &item.run.ExternalExecutionID, &item.run.ExecutorNodeID, &item.run.ExecutorAddress, &item.run.BroadcastGroupID, &item.run.ShardIndex, &item.run.ShardTotal, &item.run.RescheduleOnTerminal, &item.run.OverrideAddresses, &item.maxRetries); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, item)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, item := range expired {
		var finishedAt time.Time
		if err = tx.QueryRow(ctx, `UPDATE job_runs SET status='timed_out',finished_at=now(),error_message='callback deadline exceeded',callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='waiting_callback' RETURNING finished_at`, item.run.ID).Scan(&finishedAt); err != nil {
			return err
		}
		if err = emitRunLifecycleEventTx(ctx, tx, item.run.ID, "timed_out"); err != nil {
			return err
		}
		if item.run.Attempt <= item.maxRetries {
			if _, err = insertRetryRunTx(ctx, tx, item.run, callbackRetryDelay(item.run.Attempt)); err != nil {
				return err
			}
		} else {
			if err = emitRunEventTx(ctx, tx, item.run.ID, "timed_out", "exhausted"); err != nil {
				return err
			}
			if err = rearmFixedDelay(ctx, tx, item.run, finishedAt); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

const historyCleanupBatchSize = 10000

func (s *Store) CleanupAuxiliaryHistory(ctx context.Context, retention time.Duration) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin history cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_run_idempotency
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_run_idempotency AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean idempotency history: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_run_logs
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_run_logs AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean run logs: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM outbox_events
		WHERE published_at IS NOT NULL AND published_at<now()-$1*interval '1 second'
		ORDER BY published_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM outbox_events AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean outbox history: %w", err)
	}
	if _, err = tx.Exec(ctx, `WITH doomed AS (
		SELECT ctid FROM job_dependency_dispatches
		WHERE created_at<now()-$1*interval '1 second'
		ORDER BY created_at
		LIMIT $2 FOR UPDATE SKIP LOCKED
	) DELETE FROM job_dependency_dispatches AS history USING doomed WHERE history.ctid=doomed.ctid`, retention.Seconds(), historyCleanupBatchSize); err != nil {
		return fmt.Errorf("clean dependency dispatch history: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit history cleanup: %w", err)
	}
	return nil
}

func (s *Store) CleanupRunHistory(ctx context.Context, retention time.Duration) error {
	rows, err := s.pool.Query(ctx, `SELECT id FROM tenants ORDER BY id`)
	if err != nil {
		return fmt.Errorf("list tenants for run cleanup: %w", err)
	}
	tenantIDs := make([]string, 0)
	for rows.Next() {
		var tenantID string
		if err = rows.Scan(&tenantID); err != nil {
			rows.Close()
			return err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	before := time.Now().Add(-retention)
	for _, tenantID := range tenantIDs {
		if _, err = s.PurgeRunHistory(ctx, tenantID, "", before, 10000); err != nil {
			return fmt.Errorf("clean terminal runs for tenant %s: %w", tenantID, err)
		}
	}
	return nil
}
func (s *Store) PurgeRunHistory(ctx context.Context, tenantID, jobID string, before time.Time, limit int) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin run history purge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT id FROM job_runs WHERE tenant_id=$1 AND ($2='' OR job_id=$2::uuid) AND scheduled_at<$3 AND status IN ('succeeded','failed','timed_out','cancelled','skipped') ORDER BY scheduled_at LIMIT $4 FOR UPDATE SKIP LOCKED`, tenantID, jobID, before, limit)
	if err != nil {
		return 0, fmt.Errorf("select run history: %w", err)
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_run_logs WHERE run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete run logs: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_run_idempotency WHERE run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete run idempotency: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM job_dependency_dispatches WHERE parent_run_id=ANY($1::uuid[]) OR child_run_id=ANY($1::uuid[])`, ids); err != nil {
		return 0, fmt.Errorf("delete dependency dispatches: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM job_runs WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, tenantID, ids)
	if err != nil {
		return 0, fmt.Errorf("delete run history: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit run history purge: %w", err)
	}
	return tag.RowsAffected(), nil
}
func (s *Store) FailRun(ctx context.Context, r Run, state string, status int, errorMessage string, retryDelay *time.Duration) (*Run, error) {
	if state != "failed" && state != "timed_out" {
		return nil, fmt.Errorf("invalid failure state %q", state)
	}
	if err := requireRunLease(r); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var finishedAt time.Time
	err = tx.QueryRow(ctx, `UPDATE job_runs SET status=$2,response_status=$3,error_message=$4,finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE id=$1 AND status='running' AND lease_token=$5 RETURNING finished_at`, r.ID, state, status, errorMessage, r.LeaseToken).Scan(&finishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if err = emitRunLifecycleEventTx(ctx, tx, r.ID, state); err != nil {
		return nil, err
	}
	if retryDelay == nil {
		if err = emitRunEventTx(ctx, tx, r.ID, state, "exhausted"); err != nil {
			return nil, err
		}
	}
	var retry *Run
	if retryDelay != nil {
		next, retryErr := insertRetryRunTx(ctx, tx, r, *retryDelay)
		err = retryErr
		if err != nil {
			return nil, err
		}
		retry = &next
	}
	if retryDelay == nil {
		if err = rearmFixedDelay(ctx, tx, r, finishedAt); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return retry, nil
}

func (s *Store) IsRunRunning(ctx context.Context, runID string) (bool, error) {
	var running bool
	if err := s.pool.QueryRow(ctx, `SELECT status='running' FROM job_runs WHERE id=$1`, runID).Scan(&running); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("read run state: %w", err)
	}
	return running, nil
}

func (s *Store) CancelRun(ctx context.Context, tenantID, runID, reason string) (Run, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run, err := scanRun(tx.QueryRow(ctx, `UPDATE job_runs SET status='cancelled',finished_at=COALESCE(finished_at,now()),error_message=$3,lease_owner=NULL,lease_token=NULL,lease_until=NULL,callback_token_hash=NULL,callback_deadline=NULL WHERE tenant_id=$1 AND id=$2 AND status IN ('pending','running','waiting_callback') RETURNING `+runSelectColumns, tenantID, runID, reason))
	if err == nil {
		if err = emitRunLifecycleEventTx(ctx, tx, run.ID, "cancelled"); err != nil {
			return Run{}, err
		}
		if run.FinishedAt != nil {
			if err = rearmFixedDelay(ctx, tx, run, *run.FinishedAt); err != nil {
				return Run{}, err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return Run{}, err
		}
		return run, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Run{}, fmt.Errorf("cancel run: %w", err)
	}
	run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND id=$2`, tenantID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("read run after cancel: %w", err)
	}
	if run.Status == "cancelled" {
		if err = tx.Commit(ctx); err != nil {
			return Run{}, err
		}
		return run, nil
	}
	return Run{}, ErrNotCancellable
}

const runSelectColumns = `id,tenant_id,job_id,trigger_type,status,attempt,scheduled_at,started_at,finished_at,COALESCE(response_status,0),COALESCE(error_message,''),runtime_input,COALESCE(parent_run_id::text,''),COALESCE(retry_of_run_id::text,''),external_execution_id,COALESCE(executor_node_id,''),COALESCE(executor_address,''),COALESCE(broadcast_group_id::text,''),COALESCE(shard_index,0),COALESCE(shard_total,0),reschedule_on_terminal,override_addresses`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.StartedAt, &r.FinishedAt, &r.ResponseStatus, &r.ErrorMessage, &r.RuntimeInput, &r.ParentRunID, &r.RetryOfRunID, &r.ExternalExecutionID, &r.ExecutorNodeID, &r.ExecutorAddress, &r.BroadcastGroupID, &r.ShardIndex, &r.ShardTotal, &r.RescheduleOnTerminal, &r.OverrideAddresses)
	return r, err
}

func (s *Store) GetRun(ctx context.Context, tenantID, runID string) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND id=$2`, tenantID, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return r, err
}

func (s *Store) ListRuns(ctx context.Context, tenantID, jobID string, limit int) ([]Run, error) {
	return s.ListRunsFiltered(ctx, tenantID, jobID, "", limit)
}

func (s *Store) ListRunsFiltered(ctx context.Context, tenantID, jobID, broadcastGroupID string, limit int) ([]Run, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT `+runSelectColumns+` FROM job_runs WHERE tenant_id=$1 AND (NULLIF($2,'') IS NULL OR job_id=NULLIF($2,'')::uuid) AND (NULLIF($3,'') IS NULL OR broadcast_group_id=NULLIF($3,'')::uuid) ORDER BY scheduled_at DESC,shard_index LIMIT $4`, tenantID, jobID, broadcastGroupID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(&r.ID, &r.TenantID, &r.JobID, &r.TriggerType, &r.Status, &r.Attempt, &r.ScheduledAt, &r.StartedAt, &r.FinishedAt, &r.ResponseStatus, &r.ErrorMessage, &r.RuntimeInput, &r.ParentRunID, &r.RetryOfRunID, &r.ExternalExecutionID, &r.ExecutorNodeID, &r.ExecutorAddress, &r.BroadcastGroupID, &r.ShardIndex, &r.ShardTotal, &r.RescheduleOnTerminal, &r.OverrideAddresses); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
