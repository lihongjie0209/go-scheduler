package schedule

import (
	"testing"
	"time"
)

func TestNextCronWithTimezone(t *testing.T) {
	after := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	got, err := Next("cron", "0 0 9 * * *", "Asia/Shanghai", after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestNextCronSupportsQuartzUnspecifiedDay(t *testing.T) {
	t.Parallel()
	after := time.Date(2026, 8, 12, 0, 0, 1, 0, time.UTC)
	got, err := Next("cron", "0/5 * * * * ?", "UTC", after)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 12, 0, 0, 5, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestNextCronDSTBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		expression string
		after      time.Time
		want       time.Time
	}{
		{
			name:       "spring forward skips nonexistent local time",
			expression: "0 30 2 * * ?",
			after:      time.Date(2026, 3, 7, 8, 0, 0, 0, time.UTC),
			want:       time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC),
		},
		{
			name:       "fall back schedules both repeated local times",
			expression: "0 30 1 * * ?",
			after:      time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC),
			want:       time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Next("cron", tt.expression, "America/New_York", tt.after)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNextCronRejectsInvalidQuartzQuestionMark(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{"? * * * * *", "0 * * ? * ?", "0 * * * * * *"} {
		if _, err := Next("cron", expression, "UTC", time.Now()); err == nil {
			t.Fatalf("expression %q was accepted", expression)
		}
	}
}

func TestNextRejectsInvalidInterval(t *testing.T) {
	if _, err := Next("fixed_interval", "0", "UTC", time.Now()); err == nil {
		t.Fatal("expected error")
	}
}

func TestNextQuartzCalendarExtensions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, expression string
		after, want      time.Time
	}{
		{name: "last day leap year", expression: "0 0 9 L * ?", after: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 2, 29, 9, 0, 0, 0, time.UTC)},
		{name: "offset from last day", expression: "0 0 9 L-3 * ?", after: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 2, 26, 9, 0, 0, 0, time.UTC)},
		{name: "nearest weekday saturday", expression: "0 0 9 15W * ?", after: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 6, 14, 9, 0, 0, 0, time.UTC)},
		{name: "nearest weekday stays in month", expression: "0 0 9 1W * ?", after: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 6, 3, 9, 0, 0, 0, time.UTC)},
		{name: "last weekday", expression: "0 0 9 LW * ?", after: time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 30, 9, 0, 0, 0, time.UTC)},
		{name: "last friday", expression: "0 0 9 ? * 6L", after: time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 30, 9, 0, 0, 0, time.UTC)},
		{name: "first monday", expression: "0 0 9 ? * 2#1", after: time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 9, 2, 9, 0, 0, 0, time.UTC)},
		{name: "missing fifth weekday skips month", expression: "0 0 9 ? * 4#5", after: time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2025, 4, 30, 9, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Next("cron", tt.expression, "UTC", tt.after)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("Next(%q)=%s want=%s", tt.expression, got, tt.want)
			}
		})
	}
}

func TestQuartzCalendarExtensionValidation(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{"0 0 9 L-31 * ?", "0 0 9 32W * ?", "0 0 9 ? * 6#0", "0 0 9 ? * 6#6", "0 0 9 ? * 3#1,6#3", "0 0 9 L * MON"} {
		expression := expression
		t.Run(expression, func(t *testing.T) {
			if _, err := Next("cron", expression, "UTC", time.Now()); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNextUsesQuartzNumericDayOfWeek(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, expression string
		after, want      time.Time
	}{
		{name: "one is sunday", expression: "0 0 9 ? * 1", after: time.Date(2024, 8, 2, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 4, 9, 0, 0, 0, time.UTC)},
		{name: "seven is saturday", expression: "0 0 9 ? * 7", after: time.Date(2024, 8, 2, 0, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 3, 9, 0, 0, 0, time.UTC)},
		{name: "weekday range", expression: "0 0 9 ? * 2-6", after: time.Date(2024, 8, 4, 10, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 5, 9, 0, 0, 0, time.UTC)},
		{name: "list", expression: "0 0 9 ? * 1,7", after: time.Date(2024, 8, 4, 10, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 10, 9, 0, 0, 0, time.UTC)},
		{name: "named day unchanged", expression: "0 0 9 ? * MON", after: time.Date(2024, 8, 4, 10, 0, 0, 0, time.UTC), want: time.Date(2024, 8, 5, 9, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Next("cron", tt.expression, "UTC", tt.after)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("Next(%q)=%s want=%s", tt.expression, got, tt.want)
			}
		})
	}
	if _, err := Next("cron", "0 0 9 ? * 0", "UTC", time.Now()); err == nil {
		t.Fatal("Quartz day zero was accepted")
	}
}
