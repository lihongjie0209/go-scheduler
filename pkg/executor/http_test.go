package executor

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPHandlerSendsStableExecutionIdentity(t *testing.T) {
	t.Parallel()
	var received http.Header
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Header.Clone()
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(target.Close)

	handler := HTTPHandler(target.Client())
	err := handler(t.Context(), Task{
		RunID:               "run-2",
		ExternalExecutionID: "run-2",
		JobID:               "job-1",
		HTTP: &HTTPExecution{
			URL:    target.URL,
			Method: http.MethodPost,
			Headers: map[string]string{
				"Idempotency-Key":             "user-supplied",
				"X-Go-Scheduler-Execution-ID": "user-supplied",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for header, expected := range map[string]string{
		"Idempotency-Key":             "run-2",
		"X-Go-Scheduler-Execution-ID": "run-2",
		"X-Go-Scheduler-Run-ID":       "run-2",
		"X-Go-Scheduler-Job-ID":       "job-1",
	} {
		if actual := received.Get(header); actual != expected {
			t.Errorf("%s = %q, want %q", header, actual, expected)
		}
	}
}

func TestHTTPHandlerAwaitsCallbackOnAccepted(t *testing.T) {
	t.Parallel()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(target.Close)
	err := HTTPHandler(target.Client())(t.Context(), Task{RunID: "run-1", HTTP: &HTTPExecution{URL: target.URL, Method: http.MethodPost}})
	if !errors.Is(err, ErrAwaitingCallback) {
		t.Fatalf("error = %v, want ErrAwaitingCallback", err)
	}
}

func TestHTTPHandlerFallsBackToRunID(t *testing.T) {
	t.Parallel()
	var idempotencyKey string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)

	err := HTTPHandler(target.Client())(t.Context(), Task{RunID: "run-1", HTTP: &HTTPExecution{URL: target.URL, Method: http.MethodGet}})
	if err != nil {
		t.Fatal(err)
	}
	if idempotencyKey != "run-1" {
		t.Fatalf("Idempotency-Key = %q, want run-1", idempotencyKey)
	}
}
