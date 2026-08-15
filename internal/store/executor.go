package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type ExecutorGroup struct {
	ID, TenantID, Name, RouteStrategy, RegistrationMode string
	ManualAddresses                                     []string
	Version                                             int64
}

type ExecutorNode struct {
	GroupID, NodeID, Address string
	Labels                   []string
	ExpiresAt, UpdatedAt     time.Time
	UseCount                 int64
	LastUsedAt               time.Time
	Static                   bool
}

type ExecutorSelector func(ExecutorRoutingSnapshot) (ExecutorNode, error)

func normalizeExecutorAddress(address string) string {
	return strings.TrimRight(strings.TrimSpace(address), "/")
}

func unregisteredOverrideAddresses(nodes []ExecutorNode, addresses []string) []string {
	registered := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		registered[normalizeExecutorAddress(node.Address)] = struct{}{}
	}
	var missing []string
	for _, address := range addresses {
		if _, ok := registered[normalizeExecutorAddress(address)]; !ok {
			missing = append(missing, address)
		}
	}
	return missing
}

func (s *Store) ReserveExecutorRoute(ctx context.Context, tenantID, groupID, jobID string, selector ExecutorSelector) (ExecutorNode, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutorNode{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var snapshot ExecutorRoutingSnapshot
	err = tx.QueryRow(ctx, `SELECT g.route_strategy FROM executor_groups g JOIN jobs j ON j.executor_group_id=g.id WHERE g.tenant_id=$1 AND g.id=$2 AND j.id=$3`, tenantID, groupID, jobID).Scan(&snapshot.Strategy)
	if errors.Is(err, pgx.ErrNoRows) {
		return ExecutorNode{}, ErrNotFound
	}
	if err != nil {
		return ExecutorNode{}, err
	}
	err = tx.QueryRow(ctx, `INSERT INTO executor_job_route_counters(job_id,route_count) VALUES($1,0) ON CONFLICT(job_id) DO UPDATE SET route_count=executor_job_route_counters.route_count+1,updated_at=now() RETURNING route_count`, jobID).Scan(&snapshot.Cursor)
	if err != nil {
		return ExecutorNode{}, err
	}
	rows, err := tx.Query(ctx, `SELECT n.group_id,n.node_id,n.address,n.expires_at,n.updated_at,COALESCE(s.use_count,0),s.last_used_at,n.is_static,n.labels FROM executor_nodes n LEFT JOIN executor_job_route_state s ON s.group_id=n.group_id AND s.node_id=n.node_id AND s.job_id=$2 WHERE n.group_id=$1 AND (n.is_static OR n.expires_at>now()) ORDER BY CASE WHEN n.is_static THEN n.address ELSE n.node_id END`, groupID, jobID)
	if err != nil {
		return ExecutorNode{}, err
	}
	for rows.Next() {
		var node ExecutorNode
		var lastUsedAt *time.Time
		if err = rows.Scan(&node.GroupID, &node.NodeID, &node.Address, &node.ExpiresAt, &node.UpdatedAt, &node.UseCount, &lastUsedAt, &node.Static, &node.Labels); err != nil {
			rows.Close()
			return ExecutorNode{}, err
		}
		if lastUsedAt != nil {
			node.LastUsedAt = *lastUsedAt
		}
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return ExecutorNode{}, err
	}
	selected, err := selector(snapshot)
	if err != nil {
		return ExecutorNode{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO executor_job_route_state(job_id,group_id,node_id,use_count,last_used_at) VALUES($1,$2,$3,1,now()) ON CONFLICT(job_id,node_id) DO UPDATE SET use_count=executor_job_route_state.use_count+1,last_used_at=now(),updated_at=now()`, jobID, groupID, selected.NodeID); err != nil {
		return ExecutorNode{}, fmt.Errorf("record executor route: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ExecutorNode{}, err
	}
	return selected, nil
}

type ExecutorRoutingSnapshot struct {
	Strategy string
	Cursor   int64
	Nodes    []ExecutorNode
}

func OverrideExecutorNodes(addresses []string) []ExecutorNode {
	nodes := make([]ExecutorNode, 0, len(addresses))
	for _, address := range addresses {
		digest := sha256.Sum256([]byte(address))
		nodes = append(nodes, ExecutorNode{NodeID: fmt.Sprintf("override-%x", digest[:8]), Address: address, Static: true})
	}
	return nodes
}

func (s *Store) ExecutorRouteStrategy(ctx context.Context, tenantID, groupID, jobID string) (string, error) {
	var strategy string
	err := s.pool.QueryRow(ctx, `SELECT g.route_strategy FROM executor_groups g JOIN jobs j ON j.executor_group_id=g.id WHERE g.tenant_id=$1 AND g.id=$2 AND j.id=$3`, tenantID, groupID, jobID).Scan(&strategy)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return strategy, err
}

func (s *Store) ReserveExecutorOverrideRoute(ctx context.Context, tenantID, groupID, jobID string, addresses []string, selector ExecutorSelector) (ExecutorNode, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutorNode{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var snapshot ExecutorRoutingSnapshot
	if err = tx.QueryRow(ctx, `SELECT g.route_strategy FROM executor_groups g JOIN jobs j ON j.executor_group_id=g.id WHERE g.tenant_id=$1 AND g.id=$2 AND j.id=$3`, tenantID, groupID, jobID).Scan(&snapshot.Strategy); errors.Is(err, pgx.ErrNoRows) {
		return ExecutorNode{}, ErrNotFound
	} else if err != nil {
		return ExecutorNode{}, err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO executor_job_route_counters(job_id,route_count) VALUES($1,0) ON CONFLICT(job_id) DO UPDATE SET route_count=executor_job_route_counters.route_count+1,updated_at=now() RETURNING route_count`, jobID).Scan(&snapshot.Cursor); err != nil {
		return ExecutorNode{}, err
	}
	nodes := OverrideExecutorNodes(addresses)
	rows, err := tx.Query(ctx, `SELECT address,use_count,last_used_at FROM executor_override_route_state WHERE job_id=$1 AND address=ANY($2::text[])`, jobID, addresses)
	if err != nil {
		return ExecutorNode{}, err
	}
	state := make(map[string]ExecutorNode, len(nodes))
	for rows.Next() {
		var node ExecutorNode
		if err = rows.Scan(&node.Address, &node.UseCount, &node.LastUsedAt); err != nil {
			rows.Close()
			return ExecutorNode{}, err
		}
		state[node.Address] = node
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return ExecutorNode{}, err
	}
	for index := range nodes {
		if saved, ok := state[nodes[index].Address]; ok {
			nodes[index].UseCount = saved.UseCount
			nodes[index].LastUsedAt = saved.LastUsedAt
		}
	}
	snapshot.Nodes = nodes
	selected, err := selector(snapshot)
	if err != nil {
		return ExecutorNode{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO executor_override_route_state(job_id,address,use_count,last_used_at) VALUES($1,$2,1,now()) ON CONFLICT(job_id,address) DO UPDATE SET use_count=executor_override_route_state.use_count+1,last_used_at=now(),updated_at=now()`, jobID, selected.Address); err != nil {
		return ExecutorNode{}, fmt.Errorf("record executor override route: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return ExecutorNode{}, err
	}
	return selected, nil
}

func (s *Store) CreateExecutorGroup(ctx context.Context, group ExecutorGroup) (ExecutorGroup, error) {
	if group.RegistrationMode == "" {
		group.RegistrationMode = "automatic"
	}
	group.ID = uuid.NewString()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutorGroup{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = tx.QueryRow(ctx, `INSERT INTO executor_groups(id,tenant_id,name,route_strategy,registration_mode) VALUES($1,$2,$3,$4,$5) RETURNING version`, group.ID, group.TenantID, group.Name, group.RouteStrategy, group.RegistrationMode).Scan(&group.Version); err != nil {
		return ExecutorGroup{}, err
	}
	if err = replaceManualExecutorNodes(ctx, tx, group.ID, group.ManualAddresses); err != nil {
		return ExecutorGroup{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ExecutorGroup{}, err
	}
	return group, nil
}

func (s *Store) UpdateExecutorGroup(ctx context.Context, group ExecutorGroup) (ExecutorGroup, error) {
	if group.RegistrationMode == "" {
		group.RegistrationMode = "automatic"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutorGroup{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	err = tx.QueryRow(ctx, `UPDATE executor_groups SET name=$3,route_strategy=$4,registration_mode=$5,version=version+1,updated_at=now() WHERE tenant_id=$1 AND id=$2 AND version=$6 RETURNING version`, group.TenantID, group.ID, group.Name, group.RouteStrategy, group.RegistrationMode, group.Version).Scan(&group.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ExecutorGroup{}, ErrConflict
	}
	if err != nil {
		return ExecutorGroup{}, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM executor_nodes WHERE group_id=$1 AND is_static`, group.ID); err != nil {
		return ExecutorGroup{}, err
	}
	if group.RegistrationMode == "manual" {
		if _, err = tx.Exec(ctx, `DELETE FROM executor_nodes WHERE group_id=$1 AND NOT is_static`, group.ID); err != nil {
			return ExecutorGroup{}, err
		}
	}
	if err = replaceManualExecutorNodes(ctx, tx, group.ID, group.ManualAddresses); err != nil {
		return ExecutorGroup{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ExecutorGroup{}, err
	}
	return group, nil
}

func replaceManualExecutorNodes(ctx context.Context, tx pgx.Tx, groupID string, addresses []string) error {
	for _, address := range addresses {
		digest := sha256.Sum256([]byte(address))
		nodeID := fmt.Sprintf("manual-%x", digest[:8])
		if _, err := tx.Exec(ctx, `INSERT INTO executor_nodes(group_id,node_id,address,expires_at,is_static) VALUES($1,$2,$3,now(),true)`, groupID, nodeID, address); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DeleteExecutorGroup(ctx context.Context, tenantID, groupID string, version int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM executor_groups WHERE tenant_id=$1 AND id=$2 AND version=$3`, tenantID, groupID, version)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrExecutorGroupInUse
		}
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) ListExecutorGroups(ctx context.Context, tenantID string) ([]ExecutorGroup, error) {
	rows, err := s.pool.Query(ctx, `SELECT g.id,g.tenant_id,g.name,g.route_strategy,g.version,g.registration_mode,COALESCE(array_agg(n.address ORDER BY n.address) FILTER (WHERE n.is_static),'{}') FROM executor_groups g LEFT JOIN executor_nodes n ON n.group_id=g.id WHERE g.tenant_id=$1 GROUP BY g.id ORDER BY g.name,g.id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []ExecutorGroup
	for rows.Next() {
		var group ExecutorGroup
		if err = rows.Scan(&group.ID, &group.TenantID, &group.Name, &group.RouteStrategy, &group.Version, &group.RegistrationMode, &group.ManualAddresses); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

func (s *Store) RegisterExecutorNode(ctx context.Context, tenantID, groupID, nodeID, address string, ttl time.Duration, labelSets ...[]string) (ExecutorNode, error) {
	var node ExecutorNode
	labels := []string{}
	if len(labelSets) > 0 {
		labels = labelSets[0]
	}
	err := s.pool.QueryRow(ctx, `INSERT INTO executor_nodes(group_id,node_id,address,expires_at,is_static,labels) SELECT id,$3,$4,now()+$5*interval '1 second',false,$6 FROM executor_groups WHERE tenant_id=$1 AND id=$2 AND registration_mode='automatic' ON CONFLICT(group_id,node_id) DO UPDATE SET address=excluded.address,expires_at=excluded.expires_at,is_static=false,labels=excluded.labels,updated_at=now() RETURNING group_id,node_id,address,expires_at,updated_at,is_static,labels`, tenantID, groupID, nodeID, address, ttl.Seconds(), labels).Scan(&node.GroupID, &node.NodeID, &node.Address, &node.ExpiresAt, &node.UpdatedAt, &node.Static, &node.Labels)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if existsErr := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM executor_groups WHERE tenant_id=$1 AND id=$2)`, tenantID, groupID).Scan(&exists); existsErr != nil {
			return ExecutorNode{}, existsErr
		}
		if exists {
			return ExecutorNode{}, ErrRegistrationMode
		}
		return ExecutorNode{}, ErrNotFound
	}
	return node, err
}

func (s *Store) UnregisterExecutorNode(ctx context.Context, tenantID, groupID, nodeID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var mode string
	err = tx.QueryRow(ctx, `SELECT registration_mode FROM executor_groups WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, groupID).Scan(&mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if mode != "automatic" {
		return ErrRegistrationMode
	}
	if _, err = tx.Exec(ctx, `DELETE FROM executor_nodes WHERE group_id=$1 AND node_id=$2 AND NOT is_static`, groupID, nodeID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListExecutorNodes(ctx context.Context, tenantID, groupID string, liveOnly bool) ([]ExecutorNode, error) {
	rows, err := s.pool.Query(ctx, `SELECT n.group_id,n.node_id,n.address,n.expires_at,n.updated_at,n.is_static,n.labels FROM executor_nodes n JOIN executor_groups g ON g.id=n.group_id WHERE g.tenant_id=$1 AND g.id=$2 AND (NOT $3 OR n.is_static OR n.expires_at>now()) ORDER BY CASE WHEN n.is_static THEN n.address ELSE n.node_id END`, tenantID, groupID, liveOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []ExecutorNode
	for rows.Next() {
		var node ExecutorNode
		if err = rows.Scan(&node.GroupID, &node.NodeID, &node.Address, &node.ExpiresAt, &node.UpdatedAt, &node.Static, &node.Labels); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func (s *Store) ExecutorRouteCandidates(ctx context.Context, tenantID, groupID, jobID string) (string, []ExecutorNode, error) {
	rows, err := s.pool.Query(ctx, `SELECT g.route_strategy,n.group_id,n.node_id,n.address,n.expires_at,n.updated_at,n.is_static,n.labels
		FROM executor_groups g JOIN jobs j ON j.executor_group_id=g.id JOIN executor_nodes n ON n.group_id=g.id
		WHERE g.tenant_id=$1 AND g.id=$2 AND j.id=$3 AND (n.is_static OR n.expires_at>now())
		AND NOT EXISTS (
			SELECT 1 FROM job_executor_labels l WHERE l.job_id=j.id
			AND ((NOT l.excluded AND NOT (l.label=ANY(n.labels))) OR (l.excluded AND l.label=ANY(n.labels)))
		)
		ORDER BY CASE WHEN n.is_static THEN n.address ELSE n.node_id END`, tenantID, groupID, jobID)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	var strategy string
	var nodes []ExecutorNode
	for rows.Next() {
		var node ExecutorNode
		if err = rows.Scan(&strategy, &node.GroupID, &node.NodeID, &node.Address, &node.ExpiresAt, &node.UpdatedAt, &node.Static, &node.Labels); err != nil {
			return "", nil, err
		}
		nodes = append(nodes, node)
	}
	if err = rows.Err(); err != nil {
		return "", nil, err
	}
	if strategy == "" {
		var exists bool
		if err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM executor_groups g JOIN jobs j ON j.executor_group_id=g.id WHERE g.tenant_id=$1 AND g.id=$2 AND j.id=$3)`, tenantID, groupID, jobID).Scan(&exists); err != nil {
			return "", nil, err
		}
		if !exists {
			return "", nil, ErrNotFound
		}
	}
	return strategy, nodes, nil
}

func (s *Store) AssignClaimedRunExecutor(ctx context.Context, run Run, nodeID, address string) error {
	if err := requireRunLease(run); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE job_runs SET executor_node_id=$2,executor_address=$3 WHERE id=$1 AND status='running' AND lease_token=$4`, run.ID, nodeID, address, run.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func (s *Store) PrepareClaimedExecutorDispatch(ctx context.Context, run Run, nodeID, address string, tokenHash []byte, deadline time.Time) error {
	if err := requireRunLease(run); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `UPDATE job_runs SET executor_node_id=$2,executor_address=$3,status='waiting_callback',response_status=202,callback_token_hash=$4,callback_deadline=$5,lease_owner=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND status='running' AND lease_token=$6`, run.ID, nodeID, address, tokenHash, deadline, run.LeaseToken)
	if err != nil {
		return fmt.Errorf("prepare executor dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	if err = emitRunLifecycleEventTx(ctx, tx, run.ID, "waiting_callback"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
