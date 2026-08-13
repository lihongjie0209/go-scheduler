package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistrarHeartbeatsUntilContextCancellation(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var unregisters atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/executor-groups/group-1/nodes/node-1" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer gsk_test" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		if r.Method == http.MethodDelete {
			unregisters.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPut || body["address"] != "http://executor:9999" || body["ttl_seconds"] != float64(6) {
			t.Errorf("method=%s body=%v", r.Method, body)
		}
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()
	registrar, err := NewRegistrar(RegistrarOptions{APIURL: api.URL, Token: "gsk_test", GroupID: "group-1", NodeID: "node-1", Address: "http://executor:9999", TTL: 6 * time.Second, HTTPClient: api.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- registrar.Run(ctx) }()
	deadline := time.Now().Add(2500 * time.Millisecond)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("heartbeat calls=%d", calls.Load())
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("registrar did not stop")
	}
	if unregisters.Load() != 1 {
		t.Fatalf("unregister calls=%d", unregisters.Load())
	}
}

func TestNewRegistrarValidatesOptions(t *testing.T) {
	t.Parallel()
	valid := RegistrarOptions{APIURL: "https://scheduler.example.com", Token: "token", GroupID: "group", NodeID: "node", Address: "https://executor.example.com", TTL: 30 * time.Second}
	tests := []struct {
		name   string
		mutate func(*RegistrarOptions)
	}{{"api URL", func(o *RegistrarOptions) { o.APIURL = "scheduler" }}, {"token", func(o *RegistrarOptions) { o.Token = "" }}, {"group", func(o *RegistrarOptions) { o.GroupID = "" }}, {"node", func(o *RegistrarOptions) { o.NodeID = "" }}, {"address", func(o *RegistrarOptions) { o.Address = "file:///tmp/x" }}, {"ttl", func(o *RegistrarOptions) { o.TTL = time.Second }}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := valid
			tt.mutate(&options)
			if _, err := NewRegistrar(options); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
