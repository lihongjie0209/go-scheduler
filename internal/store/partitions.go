package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const runPartitionLockKey = "go-scheduler:job-runs-partitions"

type PartitionMaintenanceResult struct {
	Backend string
	Dropped int
}

type monthPartition struct {
	Name       string
	Start, End time.Time
}

func requiredRunPartitions(now time.Time, retention time.Duration, premake int) []monthPartition {
	now = now.UTC()
	first := now.Add(-retention)
	first = time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC)
	last := time.Date(now.Year(), now.Month()+time.Month(premake), 1, 0, 0, 0, 0, time.UTC)
	partitions := make([]monthPartition, 0, int(last.Sub(first)/(24*time.Hour*28))+1)
	for start := first; !start.After(last); start = start.AddDate(0, 1, 0) {
		end := start.AddDate(0, 1, 0)
		partitions = append(partitions, monthPartition{
			Name:  fmt.Sprintf("job_runs_y%04dm%02d", start.Year(), start.Month()),
			Start: start,
			End:   end,
		})
	}
	return partitions
}

func parseRunPartitionEnd(name string) (time.Time, bool) {
	if len(name) != len("job_runs_y2000m01") || !strings.HasPrefix(name, "job_runs_y") || name[14] != 'm' {
		return time.Time{}, false
	}
	year, yearErr := strconv.Atoi(name[10:14])
	month, monthErr := strconv.Atoi(name[15:17])
	if yearErr != nil || monthErr != nil || month < 1 || month > 12 {
		return time.Time{}, false
	}
	return time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC), true
}

// MaintainRunPartitions delegates to pg_partman when installed. On plain
// PostgreSQL it creates monthly partitions and removes expired partitions
// containing no active runs. An advisory transaction lock makes the fallback
// safe when multiple scheduler processes share the database.
func (s *Store) MaintainRunPartitions(ctx context.Context, now time.Time, retention time.Duration) (PartitionMaintenanceResult, error) {
	var partman bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_partman')`).Scan(&partman); err != nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("detect pg_partman: %w", err)
	}
	if partman {
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM partman.part_config WHERE parent_table='public.job_runs')`).Scan(&partman); err != nil {
			return PartitionMaintenanceResult{}, fmt.Errorf("detect pg_partman configuration: %w", err)
		}
	}
	if partman {
		if _, err := s.pool.Exec(ctx, `CALL partman.run_maintenance_proc()`); err != nil {
			return PartitionMaintenanceResult{}, fmt.Errorf("run pg_partman maintenance: %w", err)
		}
		return PartitionMaintenanceResult{Backend: "pg_partman"}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("begin partition maintenance: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var locked bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, runPartitionLockKey).Scan(&locked); err != nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("lock partition maintenance: %w", err)
	}
	result := PartitionMaintenanceResult{Backend: "application"}
	if !locked {
		return result, nil
	}
	for _, partition := range requiredRunPartitions(now, retention, 3) {
		identifier := pgx.Identifier{"public", partition.Name}.Sanitize()
		statement := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF public.job_runs FOR VALUES FROM (%s) TO (%s)",
			identifier,
			quoteTimestamp(partition.Start),
			quoteTimestamp(partition.End),
		)
		_, execErr := tx.Exec(ctx, statement)
		if execErr != nil {
			return PartitionMaintenanceResult{}, fmt.Errorf("create run partition %s: %w", partition.Name, execErr)
		}
	}

	rows, err := tx.Query(ctx, `SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON parent.oid=inhparent
		JOIN pg_class child ON child.oid=inhrelid
		JOIN pg_namespace namespace ON namespace.oid=child.relnamespace
		WHERE parent.oid='public.job_runs'::regclass AND namespace.nspname='public'`)
	if err != nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("list run partitions: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return PartitionMaintenanceResult{}, err
		}
		names = append(names, name)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return PartitionMaintenanceResult{}, err
	}
	cutoff := now.UTC().Add(-retention)
	for _, name := range names {
		end, valid := parseRunPartitionEnd(name)
		if !valid || end.After(cutoff) {
			continue
		}
		identifier := pgx.Identifier{"public", name}.Sanitize()
		var active bool
		if err = tx.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE status IN ('pending','running','waiting_callback'))", identifier)).Scan(&active); err != nil {
			return PartitionMaintenanceResult{}, fmt.Errorf("inspect run partition %s: %w", name, err)
		}
		if active {
			continue
		}
		if _, err = tx.Exec(ctx, "DROP TABLE "+identifier); err != nil {
			return PartitionMaintenanceResult{}, fmt.Errorf("drop run partition %s: %w", name, err)
		}
		result.Dropped++
	}
	if err = tx.Commit(ctx); err != nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("commit partition maintenance: %w", err)
	}
	return result, nil
}

func quoteTimestamp(value time.Time) string {
	return "'" + value.UTC().Format(time.RFC3339) + "'::timestamptz"
}
