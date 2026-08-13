package store

import "testing"

func TestFilterExecutorNodesRequiresAndExcludesLabels(t *testing.T) {
	t.Parallel()
	nodes := []ExecutorNode{
		{NodeID: "linux-gpu", Labels: []string{"linux", "gpu"}},
		{NodeID: "linux-cpu", Labels: []string{"linux", "cpu"}},
		{NodeID: "windows-gpu", Labels: []string{"windows", "gpu"}},
	}
	filtered := FilterExecutorNodes(nodes, []string{"linux"}, []string{"gpu"})
	if len(filtered) != 1 || filtered[0].NodeID != "linux-cpu" {
		t.Fatalf("filtered nodes = %+v", filtered)
	}
}
