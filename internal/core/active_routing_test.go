package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSelectActiveExecutorFailoverSkipsUnhealthyNode(t *testing.T) {
	t.Parallel()
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unhealthy.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer healthy.Close()
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, "failover", "job-1", []executorCandidate{{ID: "a", Address: unhealthy.URL}, {ID: "b", Address: healthy.URL}}, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "b" {
		t.Fatalf("node = %q, want b", node.ID)
	}
}

func TestSelectActiveExecutorBusyoverSendsJobAndSkipsBusyNode(t *testing.T) {
	t.Parallel()
	seenJob := make(chan string, 1)
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusConflict) }))
	defer busy.Close()
	idle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenJob <- r.Header.Get("X-Job-ID")
		w.WriteHeader(http.StatusOK)
	}))
	defer idle.Close()
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, "busyover", "job-9", []executorCandidate{{ID: "a", Address: busy.URL}, {ID: "b", Address: idle.URL}}, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "b" || <-seenJob != "job-9" {
		t.Fatalf("node = %+v", node)
	}
}

func TestSelectActiveExecutorHonorsContextAndFailsWhenAllUnavailable(t *testing.T) {
	t.Parallel()
	blocked := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer blocked.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := selectActiveExecutor(ctx, &http.Client{}, "failover", "job", []executorCandidate{{ID: "a", Address: blocked.URL}}, time.Second); err == nil {
		t.Fatal("expected unavailable executor error")
	}
}
