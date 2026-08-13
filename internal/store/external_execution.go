package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// RootRunID returns the stable execution identity shared by a run and all of
// its retries. External execution environments use it as an idempotency key.
func (s *Store) RootRunID(ctx context.Context, tenantID, runID string) (string, error) {
	var rootID string
	err := s.pool.QueryRow(ctx, `WITH RECURSIVE lineage(id,retry_of_run_id) AS (
		SELECT id,retry_of_run_id FROM job_runs WHERE tenant_id=$1 AND id=$2
		UNION ALL
		SELECT parent.id,parent.retry_of_run_id FROM job_runs parent JOIN lineage child ON child.retry_of_run_id=parent.id
	) SELECT id FROM lineage WHERE retry_of_run_id IS NULL LIMIT 1`, tenantID, runID).Scan(&rootID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return rootID, err
}
