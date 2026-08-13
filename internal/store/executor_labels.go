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
