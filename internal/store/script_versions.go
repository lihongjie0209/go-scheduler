package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type JobScriptVersion struct {
	ID, JobID, ScriptLanguage, ScriptSource, Remark string
	Revision                                        int64
	CreatedAt                                       time.Time
}

func insertJobScriptVersion(ctx context.Context, tx pgx.Tx, job Job, remark string) (JobScriptVersion, error) {
	remark = strings.TrimSpace(remark)
	if remark == "" {
		remark = "job update"
	}
	version := JobScriptVersion{ID: uuid.NewString(), JobID: job.ID, ScriptLanguage: job.ScriptLanguage, ScriptSource: job.ScriptSource, Remark: remark}
	err := tx.QueryRow(ctx, `INSERT INTO job_script_versions(id,tenant_id,job_id,revision,script_language,script_source,remark)
		SELECT $1,$2,$3,COALESCE(max(revision),0)+1,$4,$5,$6 FROM job_script_versions WHERE job_id=$3
		RETURNING revision,created_at`, version.ID, job.TenantID, job.ID, job.ScriptLanguage, job.ScriptSource, version.Remark).Scan(&version.Revision, &version.CreatedAt)
	if err != nil {
		return JobScriptVersion{}, fmt.Errorf("insert job script version: %w", err)
	}
	return version, nil
}

func (s *Store) ListJobScriptVersions(ctx context.Context, tenantID, jobID string) ([]JobScriptVersion, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE tenant_id=$1 AND id=$2 AND script_language<>'')`, tenantID, jobID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT id,job_id,revision,script_language,script_source,remark,created_at FROM job_script_versions WHERE tenant_id=$1 AND job_id=$2 ORDER BY revision DESC LIMIT 100`, tenantID, jobID)
	if err != nil {
		return nil, fmt.Errorf("list job script versions: %w", err)
	}
	defer rows.Close()
	versions := make([]JobScriptVersion, 0)
	for rows.Next() {
		var version JobScriptVersion
		if err = rows.Scan(&version.ID, &version.JobID, &version.Revision, &version.ScriptLanguage, &version.ScriptSource, &version.Remark, &version.CreatedAt); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}

func (s *Store) RollbackJobScriptVersion(ctx context.Context, tenantID, jobID, versionID string, jobVersion int64, remark string) (Job, error) {
	remark = strings.TrimSpace(remark)
	if len(remark) > 200 {
		return Job{}, fmt.Errorf("remark must not exceed 200 characters")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin script rollback: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := s.scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, jobID))
	if err != nil {
		return Job{}, err
	}
	if job.Version != jobVersion {
		return Job{}, ErrConflict
	}
	var target JobScriptVersion
	err = tx.QueryRow(ctx, `SELECT id,job_id,revision,script_language,script_source,remark,created_at FROM job_script_versions WHERE tenant_id=$1 AND job_id=$2 AND id=$3`, tenantID, jobID, versionID).Scan(&target.ID, &target.JobID, &target.Revision, &target.ScriptLanguage, &target.ScriptSource, &target.Remark, &target.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get script version: %w", err)
	}
	job.ScriptLanguage, job.ScriptSource = target.ScriptLanguage, target.ScriptSource
	job, err = s.scanJob(tx.QueryRow(ctx, `UPDATE jobs SET script_language=$3,script_source=$4,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 RETURNING `+jobColumns, tenantID, jobID, job.ScriptLanguage, job.ScriptSource))
	if err != nil {
		return Job{}, err
	}
	if remark == "" {
		remark = fmt.Sprintf("rollback to revision %d", target.Revision)
	}
	if _, err = insertJobScriptVersion(ctx, tx, job, remark); err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit script rollback: %w", err)
	}
	return job, nil
}
