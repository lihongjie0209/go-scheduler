package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServerRunsHandlerAndReportsBusyState(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.test"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("demo", func(ctx context.Context, task Task) error {
		close(started)
		<-release
		if task.Input != "payload" || task.BroadcastIndex != 1 || task.BroadcastTotal != 3 {
			t.Errorf("task=%+v", task)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	body, _ := json.Marshal(runRequest{RunID: "run-1", JobID: "job-1", Handler: "demo", Input: "payload", CallbackURL: "http://scheduler.test/api/v1/callbacks/run-1", LogURL: "http://scheduler.test/api/v1/runs/run-1/logs", CallbackToken: "token", TimeoutSeconds: 5, BroadcastIndex: 1, BroadcastTotal: 3})
	done := make(chan *http.Response, 1)
	go func() {
		response, _ := http.Post(httpServer.URL+"/run", "application/json", bytes.NewReader(body))
		done <- response
	}()
	<-started
	response, err := http.Get(httpServer.URL + "/idle?job_id=job-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("busy status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	close(release)
	response = <-done
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("run status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	response, err = http.Get(httpServer.URL + "/idle?job_id=job-1")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("idle status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
}

func TestAsyncHandlerUploadsLogsAndCompletesCallback(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var callback map[string]any
	var logs struct {
		Entries []LogEntry `json:"entries"`
	}
	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(r.URL.Path, "/logs") {
			_ = json.NewDecoder(r.Body).Decode(&logs)
		} else {
			_ = json.NewDecoder(r.Body).Decode(&callback)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer scheduler.Close()
	server, err := NewServer(Options{SchedulerURL: scheduler.URL, HTTPClient: scheduler.Client()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if err = server.HandleAsync("async", func(_ context.Context, task Task) error { defer close(done); return task.Logger.Info("working") }); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	request := runRequest{RunID: "run-2", JobID: "job-2", Handler: "async", CallbackURL: scheduler.URL + "/api/v1/callbacks/run-2", LogURL: scheduler.URL + "/api/v1/runs/run-2/logs", CallbackToken: "secret", TimeoutSeconds: 5}
	raw, _ := json.Marshal(request)
	response, err := http.Post(httpServer.URL+"/run", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("async handler did not finish")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		complete := callback["succeeded"] == true && len(logs.Entries) == 1
		mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback=%v logs=%+v", callback, logs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerRejectsUnknownHandlerAndForeignSchedulerURLs(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "https://scheduler.example.com"})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []runRequest{{Handler: "missing", CallbackURL: "https://scheduler.example.com/callback", LogURL: "https://scheduler.example.com/log"}, {Handler: "missing", CallbackURL: "http://attacker.test/callback", LogURL: "https://scheduler.example.com/log"}} {
		raw, _ := json.Marshal(request)
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(raw))
		server.ServeHTTP(recorder, req)
		if recorder.Code < 400 {
			t.Fatalf("request=%+v status=%d", request, recorder.Code)
		}
	}
}

func TestServerRecoversHandlerPanic(t *testing.T) {
	t.Parallel()
	server, err := NewServer(Options{SchedulerURL: "http://scheduler.test"})
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Handle("panic", func(context.Context, Task) error { panic("boom") }); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(runRequest{RunID: "run", JobID: "job", Handler: "panic", CallbackURL: "http://scheduler.test/callback", LogURL: "http://scheduler.test/log", CallbackToken: "token", TimeoutSeconds: 5})
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(raw)))
	if recorder.Code != http.StatusInternalServerError || !strings.Contains(recorder.Body.String(), "handler panic") {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
