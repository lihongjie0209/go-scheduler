package perfbench

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGoSchedulerLoaderCreatesEnabledCronJob(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/platform/api/v1/jobs" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer benchmark-token" || r.Header.Get("X-Tenant-ID") != "tenant-1" {
			t.Errorf("authentication headers = %v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["schedule_expression"] != "5 4 3 2 1 ?" || body["timezone"] != "UTC" || body["enabled"] != true || body["target_url"] != "https://sink.example/execute?id=event-1" || body["misfire_policy"] != "fire_once" {
			t.Errorf("body = %#v", body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"job-1"}`))
	}))
	t.Cleanup(server.Close)
	loader := &GoSchedulerLoader{BaseURL: server.URL + "/platform", Token: "benchmark-token", TenantID: "tenant-1", Client: server.Client()}
	jobID, err := loader.CreateScheduledJob(t.Context(), ScheduledJob{Name: "benchmark", EventID: "event-1", ScheduledAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), SinkURL: "https://sink.example/execute"})
	if err != nil {
		t.Fatal(err)
	}
	if jobID != "job-1" {
		t.Fatalf("job ID = %q", jobID)
	}
}
