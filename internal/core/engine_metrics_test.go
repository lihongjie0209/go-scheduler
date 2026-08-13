package core

import (
	"testing"
	"time"
)

func TestDispatchDelay(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		startedAt time.Time
		scheduled time.Time
		want      time.Duration
	}{
		{name: "worker starts after schedule", startedAt: base.Add(275 * time.Millisecond), scheduled: base, want: 275 * time.Millisecond},
		{name: "same instant", startedAt: base, scheduled: base, want: 0},
		{name: "clock skew does not produce negative metric", startedAt: base, scheduled: base.Add(time.Millisecond), want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := dispatchDelay(test.startedAt, test.scheduled); got != test.want {
				t.Fatalf("dispatchDelay() = %s, want %s", got, test.want)
			}
		})
	}
}
