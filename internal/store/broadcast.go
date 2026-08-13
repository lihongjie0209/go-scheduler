package store

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5"
)

type BroadcastShard struct {
	NodeID, Address string
	Index, Total    int32
}

func liveExecutorNodesTx(ctx context.Context, tx pgx.Tx, groupID string) ([]ExecutorNode, error) {
	rows, err := tx.Query(ctx, `SELECT group_id,node_id,address,expires_at,updated_at,labels FROM executor_nodes WHERE group_id=$1 AND (is_static OR expires_at>now()) ORDER BY node_id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []ExecutorNode
	for rows.Next() {
		var node ExecutorNode
		if err = rows.Scan(&node.GroupID, &node.NodeID, &node.Address, &node.ExpiresAt, &node.UpdatedAt, &node.Labels); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

func planBroadcastShards(nodes []ExecutorNode) []BroadcastShard {
	sorted := append([]ExecutorNode(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeID < sorted[j].NodeID })
	shards := make([]BroadcastShard, 0, len(sorted))
	for index, node := range sorted {
		shards = append(shards, BroadcastShard{
			NodeID: node.NodeID, Address: node.Address,
			Index: int32(index), Total: int32(len(sorted)), // #nosec G115 -- executor groups are bounded to 100 nodes.
		})
	}
	return shards
}
