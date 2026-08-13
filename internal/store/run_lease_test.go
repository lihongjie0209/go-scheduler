package store

import "testing"

func TestRequireRunLease(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  Run
		ok   bool
	}{
		{name: "valid", run: Run{ID: "run-id", LeaseToken: "lease-token"}, ok: true},
		{name: "missing run ID", run: Run{LeaseToken: "lease-token"}},
		{name: "missing lease token", run: Run{ID: "run-id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := requireRunLease(tt.run)
			if tt.ok && err != nil {
				t.Fatalf("requireRunLease() error = %v", err)
			}
			if !tt.ok && err != ErrConflict {
				t.Fatalf("requireRunLease() error = %v, want %v", err, ErrConflict)
			}
		})
	}
}
