package core

import (
	"testing"
	"time"
)

func TestSelectExecutorNode(t *testing.T) {
	t.Parallel()
	now := time.Now()
	nodes := []executorCandidate{{ID: "b", Address: "http://b", UseCount: 4, LastUsedAt: now.Add(-time.Minute)}, {ID: "a", Address: "http://a", UseCount: 5, LastUsedAt: now}, {ID: "c", Address: "http://c", UseCount: 3, LastUsedAt: now.Add(-2 * time.Minute)}}
	tests := []struct {
		name     string
		strategy string
		cursor   int64
		random   uint64
		key      string
		wantID   string
	}{
		{name: "first is stable by node id", strategy: "first", wantID: "a"},
		{name: "last is stable by node id", strategy: "last", wantID: "c"},
		{name: "round first slot", strategy: "round", cursor: 0, wantID: "a"},
		{name: "round wraps", strategy: "round", cursor: 4, wantID: "b"},
		{name: "random uses supplied entropy", strategy: "random", random: 5, wantID: "c"},
		{name: "consistent hash uses XXL virtual ring", strategy: "hash", key: "job-2", wantID: "c"},
		{name: "least frequently used", strategy: "lfu", wantID: "c"},
		{name: "least recently used", strategy: "lru", wantID: "c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := selectExecutorNode(nodes, tt.strategy, tt.cursor, tt.random, tt.key)
			if err != nil {
				t.Fatal(err)
			}
			if got.ID != tt.wantID {
				t.Fatalf("node = %q, want %q", got.ID, tt.wantID)
			}
		})
	}
}

func TestSelectExecutorNodeRejectsNoLiveNodesAndUnknownStrategy(t *testing.T) {
	t.Parallel()
	if _, err := selectExecutorNode(nil, "first", 0, 0, ""); err == nil {
		t.Fatal("expected empty candidates to fail")
	}
	if _, err := selectExecutorNode([]executorCandidate{{ID: "a"}}, "unknown", 0, 0, ""); err == nil {
		t.Fatal("expected unsupported strategy to fail")
	}
}
