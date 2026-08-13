package schedule

import (
	"testing"
	"time"
)

func TestDueMisfirePolicies(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 10, 0, time.UTC)
	first := now.Add(-10 * time.Second)
	tests := []struct {
		name, policy string
		max, want    int
	}{{"skip", "skip", 10, 0}, {"fire once", "fire_once", 10, 1}, {"limited catch up", "catch_up", 3, 3}}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			due, next, err := Due("fixed_interval", "1", "UTC", tt.policy, first, now, tt.max)
			if err != nil {
				t.Fatal(err)
			}
			if len(due) != tt.want {
				t.Fatalf("got %d occurrences want %d", len(due), tt.want)
			}
			if !next.After(now) {
				t.Fatalf("next occurrence %s is not in future", next)
			}
		})
	}
}

func TestDueMisfirePreservesPolicyTimestampsAndAdvancesPastBacklog(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 10, 0, time.UTC)
	first := now.Add(-10 * time.Second)

	fired, next, err := Due("fixed_rate", "1", "UTC", "fire_once", first, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 || !fired[0].Equal(now) || !next.Equal(now.Add(time.Second)) {
		t.Fatalf("fire_once due=%v next=%s", fired, next)
	}

	caughtUp, next, err := Due("fixed_rate", "1", "UTC", "catch_up", first, now, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{first, first.Add(time.Second), first.Add(2 * time.Second)}
	if len(caughtUp) != len(want) {
		t.Fatalf("catch_up due=%v", caughtUp)
	}
	for index := range want {
		if !caughtUp[index].Equal(want[index]) {
			t.Fatalf("catch_up[%d]=%s want %s", index, caughtUp[index], want[index])
		}
	}
	if !next.Equal(now.Add(time.Second)) {
		t.Fatalf("catch_up next=%s want %s", next, now.Add(time.Second))
	}
}

func TestDueFixedDelayNeverCatchesUpOrPrecomputesNext(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 10, 0, time.UTC)
	due, next, err := Due("fixed_delay", "5", "UTC", "catch_up", now.Add(-time.Minute), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || !next.IsZero() {
		t.Fatalf("due=%v next=%s, want one run and no precomputed next", due, next)
	}
}

func TestDueFixedDelaySkipMisfireSchedulesFromNow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 10, 0, time.UTC)
	due, next, err := Due("fixed_delay", "5", "UTC", "skip", now.Add(-time.Minute), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 || !next.Equal(now.Add(5*time.Second)) {
		t.Fatalf("due=%v next=%s", due, next)
	}
}
