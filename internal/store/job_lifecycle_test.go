package store

import (
	"testing"
	"time"
)

func TestNextRunForEnabledState(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	job := Job{ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC"}

	tests := []struct {
		name    string
		enabled bool
		want    *time.Time
	}{
		{name: "start computes next run", enabled: true, want: timePointer(now.Add(time.Minute))},
		{name: "stop clears next run", enabled: false, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := nextRunForEnabledState(job, tt.enabled, now)
			if err != nil {
				t.Fatal(err)
			}
			if tt.want == nil && got != nil {
				t.Fatalf("next run = %v, want nil", got)
			}
			if tt.want != nil && (got == nil || !got.Equal(*tt.want)) {
				t.Fatalf("next run = %v, want %v", got, tt.want)
			}
		})
	}
}

func timePointer(value time.Time) *time.Time { return &value }
