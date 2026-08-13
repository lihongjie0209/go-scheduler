package store

import "testing"

func TestDecideBlockAction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		policy     string
		hasActive  bool
		wantAction blockAction
	}{
		{name: "serial queues behind active run", policy: "serial", hasActive: true, wantAction: blockEnqueue},
		{name: "discard later skips new run when active", policy: "discard_later", hasActive: true, wantAction: blockSkip},
		{name: "discard later enqueues when idle", policy: "discard_later", hasActive: false, wantAction: blockEnqueue},
		{name: "cover early cancels old work when active", policy: "cover_early", hasActive: true, wantAction: blockCancelAndEnqueue},
		{name: "cover early enqueues when idle", policy: "cover_early", hasActive: false, wantAction: blockEnqueue},
		{name: "parallel always enqueues", policy: "parallel", hasActive: true, wantAction: blockEnqueue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := decideBlockAction(tt.policy, tt.hasActive); got != tt.wantAction {
				t.Fatalf("action = %q, want %q", got, tt.wantAction)
			}
		})
	}
}
