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
	done := make(chan int, 1)
	go func() {
		request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/run", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			done <- 0
			return
		}
		defer func() { _ = response.Body.Close() }()
		done <- response.StatusCode
	}()
	<-started
	idleRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/idle?job_id=job-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(idleRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("busy status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
	close(release)
	statusCode := <-done
	if statusCode != http.StatusNoContent {
		t.Fatalf("run status=%d", statusCode)
	}
	idleRequest, err = http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/idle?job_id=job-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(idleRequest)
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
	httpRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/run", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(httpRequest)
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
