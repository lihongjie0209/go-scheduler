package store

import (
	"context"
	"strings"
	"testing"
)

func TestNewRejectsInvalidPoolSizeBeforeConnecting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxConns int32
		minConns int32
	}{
		{name: "zero max", maxConns: 0, minConns: 0},
		{name: "negative min", maxConns: 1, minConns: -1},
		{name: "min exceeds max", maxConns: 1, minConns: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(context.Background(), "postgres://unused", WithPoolSize(test.maxConns, test.minConns))
			if err == nil || !strings.Contains(err.Error(), "invalid database pool size") {
				t.Fatalf("New() error = %v", err)
			}
		})
	}
}
