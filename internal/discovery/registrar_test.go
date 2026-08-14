package discovery

import (
	"context"
	"errors"
	"testing"
)

func TestNoopRegistrar_RunStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- NewNoopRegistrar().Run(ctx) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestNoopRegistrar_Close(t *testing.T) {
	if err := NewNoopRegistrar().Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
