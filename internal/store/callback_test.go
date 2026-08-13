package store

import (
	"testing"
	"time"
)

func TestCallbackRetryDelayIsBoundedExponential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "first retry", attempt: 1, want: time.Second},
		{name: "second retry", attempt: 2, want: 2 * time.Second},
		{name: "bounded", attempt: 20, want: 64 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := callbackRetryDelay(tt.attempt); got != tt.want {
				t.Fatalf("callbackRetryDelay(%d) = %s, want %s", tt.attempt, got, tt.want)
			}
		})
	}
}
