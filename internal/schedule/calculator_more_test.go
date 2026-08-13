package schedule

import (
	"testing"
	"time"
)

func TestNextOnceInPastIsExhausted(t *testing.T) {
	t.Parallel()
	after := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	got, err := Next("once", "2026-01-01T00:00:00Z", "UTC", after)
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero, got %s", got)
	}
}
func TestNextFixedInterval(t *testing.T) {
	t.Parallel()
	after := time.Unix(100, 0)
	got, err := Next("fixed_interval", "30", "UTC", after)
	if err != nil {
		t.Fatal(err)
	}
	if want := after.Add(30 * time.Second); !got.Equal(want) {
		t.Fatalf("got %s want %s", got, want)
	}
}
func TestNextFixedRateAndDelay(t *testing.T) {
	t.Parallel()
	after := time.Unix(100, 0)
	for _, scheduleType := range []string{"fixed_rate", "fixed_delay"} {
		t.Run(scheduleType, func(t *testing.T) {
			got, err := Next(scheduleType, "30", "UTC", after)
			if err != nil {
				t.Fatal(err)
			}
			if want := after.Add(30 * time.Second); !got.Equal(want) {
				t.Fatalf("got %s want %s", got, want)
			}
		})
	}
}
func TestNextRejectsUnknownSchedule(t *testing.T) {
	t.Parallel()
	if _, err := Next("random", "x", "UTC", time.Now()); err == nil {
		t.Fatal("expected error")
	}
}

func TestPreview(t *testing.T) {
	t.Parallel()
	after := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	got, err := Preview("cron", "0 0 9 L * ?", "UTC", after, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || !got[0].Equal(time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)) || !got[4].Equal(time.Date(2026, 12, 31, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("preview=%v", got)
	}
	once, err := Preview("once", "2026-08-14T09:00:00Z", "UTC", after, 5)
	if err != nil || len(once) != 1 {
		t.Fatalf("once=%v err=%v", once, err)
	}
	for _, count := range []int{0, 101} {
		if _, err = Preview("fixed_rate", "60", "UTC", after, count); err == nil {
			t.Fatalf("count %d accepted", count)
		}
	}
}
