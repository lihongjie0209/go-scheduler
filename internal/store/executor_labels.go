package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func replaceJobExecutorLabels(ctx context.Context, tx pgx.Tx, jobID string, required, excluded []string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM job_executor_labels WHERE job_id=$1`, jobID); err != nil {
		return err
	}
	for _, group := range []struct {
		labels   []string
		excluded bool
	}{{required, false}, {excluded, true}} {
		for _, label := range group.labels {
			if _, err := tx.Exec(ctx, `INSERT INTO job_executor_labels(job_id,label,excluded) VALUES($1,$2,$3)`, jobID, label, group.excluded); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) JobExecutorLabels(ctx context.Context, jobID string) ([]string, []string, error) {
	return jobExecutorLabels(ctx, s.pool, jobID)
}

func (s *Store) attachJobExecutorLabels(ctx context.Context, jobs []Job) error {
	if len(jobs) == 0 {
		return nil
	}
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	required, excluded, err := jobExecutorLabelsByIDs(ctx, s.pool, ids)
	if err != nil {
		return err
	}
	applyJobExecutorLabels(jobs, required, excluded)
	return nil
}

func applyJobExecutorLabels(jobs []Job, required, excluded map[string][]string) {
	for i := range jobs {
		jobs[i].RequiredExecutorLabels = required[jobs[i].ID]
		jobs[i].ExcludedExecutorLabels = excluded[jobs[i].ID]
	}
}

func jobExecutorLabelsByIDsQuery() string {
	return `SELECT job_id::text,label,excluded FROM job_executor_labels WHERE job_id=ANY($1::uuid[]) ORDER BY job_id,excluded,label`
}

func jobExecutorLabelsByIDs(ctx context.Context, query labelQuerier, jobIDs []string) (map[string][]string, map[string][]string, error) {
	required := make(map[string][]string, len(jobIDs))
	excluded := make(map[string][]string, len(jobIDs))
	if len(jobIDs) == 0 {
		return required, excluded, nil
	}
	rows, err := query.Query(ctx, jobExecutorLabelsByIDsQuery(), jobIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var jobID, label string
		var isExcluded bool
		if err = rows.Scan(&jobID, &label, &isExcluded); err != nil {
			return nil, nil, err
		}
		if isExcluded {
			excluded[jobID] = append(excluded[jobID], label)
		} else {
			required[jobID] = append(required[jobID], label)
		}
	}
	return required, excluded, rows.Err()
}

type labelQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func jobExecutorLabels(ctx context.Context, query labelQuerier, jobID string) ([]string, []string, error) {
	rows, err := query.Query(ctx, `SELECT label,excluded FROM job_executor_labels WHERE job_id=$1 ORDER BY excluded,label`, jobID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var required, excluded []string
	for rows.Next() {
		var label string
		var isExcluded bool
		if err = rows.Scan(&label, &isExcluded); err != nil {
			return nil, nil, err
		}
		if isExcluded {
			excluded = append(excluded, label)
		} else {
			required = append(required, label)
		}
	}
	return required, excluded, rows.Err()
}

func FilterExecutorNodes(nodes []ExecutorNode, required, excluded []string) []ExecutorNode {
	filtered := make([]ExecutorNode, 0, len(nodes))
	for _, node := range nodes {
		labels := make(map[string]struct{}, len(node.Labels))
		for _, label := range node.Labels {
			labels[label] = struct{}{}
		}
		matches := true
		for _, label := range required {
			if _, exists := labels[label]; !exists {
				matches = false
				break
			}
		}
		if matches {
			for _, label := range excluded {
				if _, exists := labels[label]; exists {
					matches = false
					break
				}
			}
		}
		if matches {
			filtered = append(filtered, node)
		}
	}
	return filtered
}
