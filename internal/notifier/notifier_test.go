package notifier

import (
	"testing"
	"time"
)

func TestRetryDelayIsBoundedExponential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt int
		want    time.Duration
	}{{1, 2 * time.Second}, {2, 4 * time.Second}, {8, 256 * time.Second}, {20, 256 * time.Second}}
	for _, tt := range tests {
		if got := retryDelay(tt.attempt); got != tt.want {
			t.Fatalf("retryDelay(%d)=%s want %s", tt.attempt, got, tt.want)
		}
	}
}
