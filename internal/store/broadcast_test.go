package store

import "testing"

func TestPlanBroadcastShardsUsesStableNodeOrder(t *testing.T) {
	t.Parallel()
	nodes := []ExecutorNode{{NodeID: "node-c", Address: "http://c"}, {NodeID: "node-a", Address: "http://a"}, {NodeID: "node-b", Address: "http://b"}}
	shards := planBroadcastShards(nodes)
	if len(shards) != 3 {
		t.Fatalf("shards = %+v", shards)
	}
	for index, want := range []string{"node-a", "node-b", "node-c"} {
		if shards[index].NodeID != want || int(shards[index].Index) != index || shards[index].Total != 3 {
			t.Fatalf("shard %d = %+v", index, shards[index])
		}
	}
}

func TestPlanBroadcastShardsRejectsNoNodes(t *testing.T) {
	t.Parallel()
	if shards := planBroadcastShards(nil); len(shards) != 0 {
		t.Fatalf("shards = %+v", shards)
	}
}
