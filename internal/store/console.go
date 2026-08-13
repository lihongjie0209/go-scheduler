package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type UserSummary struct {
	ID, Email               string
	PlatformAdmin, Disabled bool
	CreatedAt               time.Time
}
type TenantSummary struct {
	ID, Name          string
	MaxConcurrentRuns int
	CreatedAt         time.Time
}
type MemberSummary struct {
	UserID, Email, Role string
	Disabled            bool
}
type Dashboard struct {
	Jobs, EnabledJobs, PendingRuns, RunningRuns, Succeeded24H, Failed24H int64
	RecentFailures                                                       []Run
	Upcoming                                                             []Job
}
type RunReportPoint struct {
	Date                                                 string
	Total, Succeeded, Failed, Active, Cancelled, Skipped int64
}

func (s *Store) RunReport(ctx context.Context, tenantID string, from, to time.Time, timezone string) ([]RunReportPoint, error) {
	rows, err := s.pool.Query(ctx, `
WITH days AS (
  SELECT generate_series($2::date, $3::date, interval '1 day')::date AS day
), counts AS (
  SELECT timezone($4, scheduled_at)::date AS day,
         count(*) AS total,
         count(*) FILTER (WHERE status='succeeded') AS succeeded,
         count(*) FILTER (WHERE status IN ('failed','timed_out')) AS failed,
         count(*) FILTER (WHERE status IN ('pending','running','waiting_callback')) AS active,
         count(*) FILTER (WHERE status='cancelled') AS cancelled,
         count(*) FILTER (WHERE status='skipped') AS skipped
    FROM job_runs
   WHERE tenant_id=$1
     AND scheduled_at >= ($2::date::timestamp AT TIME ZONE $4)
     AND scheduled_at < (($3::date + 1)::timestamp AT TIME ZONE $4)
   GROUP BY 1
)
SELECT to_char(days.day, 'YYYY-MM-DD'), coalesce(total,0), coalesce(succeeded,0),
       coalesce(failed,0), coalesce(active,0), coalesce(cancelled,0), coalesce(skipped,0)
  FROM days LEFT JOIN counts USING(day) ORDER BY days.day`, tenantID, from.Format(time.DateOnly), to.Format(time.DateOnly), timezone)
	if err != nil {
		return nil, fmt.Errorf("run report: %w", err)
	}
	defer rows.Close()
	out := make([]RunReportPoint, 0, int(to.Sub(from).Hours()/24)+1)
	for rows.Next() {
		var point RunReportPoint
		if err = rows.Scan(&point.Date, &point.Total, &point.Succeeded, &point.Failed, &point.Active, &point.Cancelled, &point.Skipped); err != nil {
			return nil, err
		}
		out = append(out, point)
	}
	return out, rows.Err()
}

func (s *Store) ListUsers(ctx context.Context) ([]UserSummary, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,email,platform_admin,disabled,created_at FROM users ORDER BY created_at DESC LIMIT 200`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()
	out := make([]UserSummary, 0)
	for rows.Next() {
		var x UserSummary
		if err = rows.Scan(&x.ID, &x.Email, &x.PlatformAdmin, &x.Disabled, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) SetUserDisabled(ctx context.Context, id string, disabled bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE users SET disabled=$2 WHERE id=$1`, id, disabled)
	return err
}
func (s *Store) ListTenants(ctx context.Context) ([]TenantSummary, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,name,max_concurrent_runs,created_at FROM tenants ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]TenantSummary, 0)
	for rows.Next() {
		var x TenantSummary
		if err = rows.Scan(&x.ID, &x.Name, &x.MaxConcurrentRuns, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) TenantMembers(ctx context.Context, tenantID string) ([]MemberSummary, error) {
	rows, err := s.pool.Query(ctx, `SELECT u.id,u.email,tm.role,u.disabled FROM tenant_memberships tm JOIN users u ON u.id=tm.user_id WHERE tm.tenant_id=$1 ORDER BY u.email`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]MemberSummary, 0)
	for rows.Next() {
		var x MemberSummary
		if err = rows.Scan(&x.UserID, &x.Email, &x.Role, &x.Disabled); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) DeleteMembership(ctx context.Context, tenantID, userID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var tenantExists bool
	if err = tx.QueryRow(ctx, `SELECT true FROM tenants WHERE id=$1 FOR UPDATE`, tenantID).Scan(&tenantExists); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var owners int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM tenant_memberships WHERE tenant_id=$1 AND role='owner'`, tenantID).Scan(&owners); err != nil {
		return err
	}
	var role string
	if err = tx.QueryRow(ctx, `SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID).Scan(&role); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if role == "owner" && owners <= 1 {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2`, tenantID, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Dashboard(ctx context.Context, tenantID string) (Dashboard, error) {
	var d Dashboard
	err := s.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE enabled) FROM jobs WHERE tenant_id=$1`, tenantID).Scan(&d.Jobs, &d.EnabledJobs)
	if err != nil {
		return d, err
	}
	err = s.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='pending'),count(*) FILTER(WHERE status IN ('running','waiting_callback')),count(*) FILTER(WHERE status='succeeded' AND scheduled_at>=now()-interval '24 hours'),count(*) FILTER(WHERE status IN ('failed','timed_out') AND scheduled_at>=now()-interval '24 hours') FROM job_runs WHERE tenant_id=$1`, tenantID).Scan(&d.PendingRuns, &d.RunningRuns, &d.Succeeded24H, &d.Failed24H)
	if err != nil {
		return d, err
	}
	runs, err := s.ListRuns(ctx, tenantID, "", 20)
	if err != nil {
		return d, err
	}
	for _, r := range runs {
		if r.Status == "failed" || r.Status == "timed_out" {
			d.RecentFailures = append(d.RecentFailures, r)
			if len(d.RecentFailures) == 5 {
				break
			}
		}
	}
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id=$1 AND enabled AND next_run_at IS NOT NULL ORDER BY next_run_at LIMIT 12`, tenantID)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		j, e := s.scanJob(rows)
		if e != nil {
			return d, e
		}
		d.Upcoming = append(d.Upcoming, j)
	}
	return d, rows.Err()
}
