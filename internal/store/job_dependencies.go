package store

import (
	"context"
	"fmt"
)

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
