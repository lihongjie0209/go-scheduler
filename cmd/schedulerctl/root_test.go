package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDashboardWithPasswordAuthentication(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "jwt-token"})
	})
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer jwt-token" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenants": []map[string]string{{"ID": "tenant-1"}}})
	})
	mux.HandleFunc("/api/v1/dashboard", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Tenant-ID") != "tenant-1" {
			t.Errorf("tenant=%q", r.Header.Get("X-Tenant-ID"))
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"Jobs": 3})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	for _, key := range []string{"SCHEDULER_URL", "SCHEDULER_TOKEN", "SCHEDULER_EMAIL", "SCHEDULER_PASSWORD", "SCHEDULER_TENANT"} {
		t.Setenv(key, "")
	}
	command := newRootCommand("test")
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetErr(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--email", "dev@example.com", "--password", "secret", "dashboard"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"Jobs": 3`) {
		t.Fatalf("output=%s", output.String())
	}
}

func TestReportsRunsBuildsDateRangeQuery(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/reports/runs" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if got := r.URL.Query().Get("from"); got != "2026-08-01" {
			t.Errorf("from=%q", got)
		}
		if got := r.URL.Query().Get("to"); got != "2026-08-13" {
			t.Errorf("to=%q", got)
		}
		if got := r.URL.Query().Get("timezone"); got != "Asia/Shanghai" {
			t.Errorf("timezone=%q", got)
		}
		_, _ = w.Write([]byte(`{"points":[]}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "reports", "runs", "--from", "2026-08-01", "--to", "2026-08-13", "--timezone", "Asia/Shanghai"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestRunsPurgeBuildsRequest(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/purge" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["before"] != "2026-08-01T00:00:00Z" || body["job_id"] != "job-1" || body["limit"] != float64(250) {
			t.Errorf("body=%v", body)
		}
		_, _ = w.Write([]byte(`{"deleted":"2"}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "runs", "purge", "--before", "2026-08-01T00:00:00Z", "--job", "job-1", "--limit", "250"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestTokenAuthenticationDoesNotRequireTenantForAPIKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/jobs" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer gsk_test" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Tenant-ID") != "" {
			t.Errorf("unexpected tenant header")
		}
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "list"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsStartUsesDedicatedLifecycleEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/jobs/job-1/start" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Version int64 `json:"version"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Version != 7 {
			t.Errorf("version = %d, want 7", body.Version)
		}
		_, _ = w.Write([]byte(`{"id":"job-1","enabled":true,"version":"8"}`))
	}))
	defer server.Close()

	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "start", "job-1", "--version", "7"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsUpdateUsesDefinitionFromStdin(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/jobs/job-1" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Name    string `json:"name"`
			Version int64  `json:"version"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Name != "updated job" || body.Version != 7 {
			t.Errorf("body = %+v", body)
		}
		_, _ = w.Write([]byte(`{"id":"job-1","name":"updated job","version":"8"}`))
	}))
	defer server.Close()

	command := newRootCommand("test")
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetIn(strings.NewReader(`{"name":"updated job","version":7}`))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "update", "job-1", "--file", "-"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "updated job") {
		t.Fatalf("output = %s", output.String())
	}
}

func TestRunsCancelUsesDedicatedEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/run-1/cancel" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Reason != "maintenance" {
			t.Errorf("reason = %q", body.Reason)
		}
		_, _ = w.Write([]byte(`{"id":"run-1","status":"cancelled"}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "runs", "cancel", "run-1", "--reason", "maintenance"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsTriggerSendsExecutorAddressOverrides(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Addresses []string `json:"override_addresses"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Join(body.Addresses, ",") != "http://worker-a:9999,http://worker-b:9999" {
			t.Errorf("addresses=%v", body.Addresses)
		}
		_, _ = w.Write([]byte(`{"id":"run-1","status":"pending"}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "trigger", "job-1", "--address", "http://worker-a:9999", "--address", "http://worker-b:9999"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsPreviewSendsScheduleDefinition(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/schedules/preview" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		var body struct {
			ScheduleType       string `json:"schedule_type"`
			ScheduleExpression string `json:"schedule_expression"`
			Timezone           string `json:"timezone"`
			After              string `json:"after"`
			Count              int    `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.ScheduleType != "cron" || body.ScheduleExpression != "0 0 9 L * ?" || body.Timezone != "Asia/Shanghai" || body.After != "2026-08-13T00:00:00Z" || body.Count != 7 {
			t.Errorf("body=%+v", body)
		}
		_, _ = w.Write([]byte(`{"trigger_times":["2026-08-31T01:00:00Z"]}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "preview", "--type", "cron", "--expression", "0 0 9 L * ?", "--timezone", "Asia/Shanghai", "--after", "2026-08-13T00:00:00Z", "--count", "7"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "2026-08-31T01:00:00Z") {
		t.Fatalf("output=%s", output.String())
	}
}

func TestExecutorsUnregisterUsesDeleteEndpoint(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/executor-groups/group-1/nodes/node-1" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "executors", "unregister", "group-1", "node-1"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestExecutorGroupsManualLifecycleRequests(t *testing.T) {
	t.Parallel()
	requests := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		requests <- r.Method + " " + r.URL.Path
		switch r.Method {
		case http.MethodPost:
			addresses, _ := body["manual_addresses"].([]any)
			if body["registration_mode"] != "manual" || len(addresses) != 2 {
				t.Errorf("create body=%v", body)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"group-1","version":"1"}`))
		case http.MethodPut:
			if body["version"] != float64(1) || body["route_strategy"] != "first" {
				t.Errorf("update body=%v", body)
			}
			_, _ = w.Write([]byte(`{"id":"group-1","version":"2"}`))
		case http.MethodDelete:
			if r.URL.Query().Get("version") != "2" {
				t.Errorf("delete query=%s", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	commands := [][]string{
		{"--server", server.URL, "--token", "gsk_test", "executors", "groups", "create", "--name", "manual", "--strategy", "round", "--mode", "manual", "--address", "http://worker-a:9999", "--address", "http://worker-b:9999"},
		{"--server", server.URL, "--token", "gsk_test", "executors", "groups", "update", "group-1", "--name", "manual", "--strategy", "first", "--mode", "manual", "--address", "http://worker-a:9999", "--version", "1"},
		{"--server", server.URL, "--token", "gsk_test", "executors", "groups", "delete", "group-1", "--version", "2"},
	}
	for _, args := range commands {
		command := newRootCommand("test")
		command.SetOut(new(bytes.Buffer))
		command.SetArgs(args)
		if err := command.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	close(requests)
	var got []string
	for request := range requests {
		got = append(got, request)
	}
	want := []string{"POST /api/v1/executor-groups", "PUT /api/v1/executor-groups/group-1", "DELETE /api/v1/executor-groups/group-1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("requests=%v want=%v", got, want)
	}
}

func TestRunsGetUsesDedicatedEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"run-1","retryOfRunId":"run-0"}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	output := new(bytes.Buffer)
	command.SetOut(output)
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "runs", "get", "run-1"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "retryOfRunId") {
		t.Fatalf("output=%s", output.String())
	}
}

func TestRunsFiltersByBroadcastGroup(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs" || r.URL.Query().Get("broadcast_group_id") != "broadcast-1" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"runs":[]}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "runs", "--broadcast-group", "broadcast-1"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestRunsLogsUsesCursorEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/run-1/logs" || r.URL.Query().Get("after") != "42" || r.URL.Query().Get("limit") != "25" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"entries":[],"next_cursor":42}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "runs", "logs", "run-1", "--after", "42", "--limit", "25"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorsRegisterUsesHeartbeatEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/executor-groups/group-1/nodes/node-1" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Address    string   `json:"address"`
			TTLSeconds int32    `json:"ttl_seconds"`
			Labels     []string `json:"labels"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Address != "http://executor:9999" || body.TTLSeconds != 30 || strings.Join(body.Labels, ",") != "linux,gpu" {
			t.Errorf("body = %+v", body)
		}
		_, _ = w.Write([]byte(`{"id":"node-1","online":true}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "executors", "register", "group-1", "node-1", "--address", "http://executor:9999", "--ttl", "30", "--label", "linux", "--label", "gpu"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsDependenciesSetUsesDedicatedEndpoint(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/jobs/parent/dependencies" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			ChildJobIDs []string `json:"child_job_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.ChildJobIDs) != 2 || body.ChildJobIDs[0] != "child-a" || body.ChildJobIDs[1] != "child-b" {
			t.Errorf("children = %v", body.ChildJobIDs)
		}
		_, _ = w.Write([]byte(`{"child_job_ids":["child-a","child-b"]}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "dependencies", "set", "parent", "--child", "child-a", "--child", "child-b"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestJobsScriptVersionsListAndRollbackUseDedicatedEndpoints(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/jobs/job-1/script-versions" {
				t.Errorf("list request = %s %s", r.Method, r.URL.Path)
			}
		case 2:
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/jobs/job-1/script-versions/version-1/rollback" {
				t.Errorf("rollback request = %s %s", r.Method, r.URL.Path)
			}
			var body struct {
				Version int64  `json:"version"`
				Remark  string `json:"remark"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Version != 7 || body.Remark != "restore stable" {
				t.Errorf("rollback body = %+v, %v", body, err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetErr(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "script-versions", "list", "job-1"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	command = newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetErr(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "jobs", "script-versions", "rollback", "job-1", "version-1", "--version", "7", "--remark", "restore stable"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationsCreateUsesAPI(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/notification-channels" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Kind   string          `json:"kind"`
			Name   string          `json:"name"`
			Config json.RawMessage `json:"config"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Kind != "webhook" || body.Name != "ops" || !bytes.Contains(body.Config, []byte("https://hooks.example.com")) {
			t.Errorf("body = %+v", body)
		}
		_, _ = w.Write([]byte(`{"id":"channel-1"}`))
	}))
	defer server.Close()
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "notifications", "create", "--kind", "webhook", "--name", "ops", "--config", `{"url":"https://hooks.example.com"}`})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}

func TestNotificationsHistoryUsesFilters(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/notification-history" || r.URL.Query().Get("channel_id") != "channel-1" || r.URL.Query().Get("job_id") != "job-1" || r.URL.Query().Get("status") != "dead" || r.URL.Query().Get("limit") != "25" || r.URL.Query().Get("cursor") != "next-page" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"deliveries":[]}`))
	}))
	t.Cleanup(server.Close)
	command := newRootCommand("test")
	command.SetOut(new(bytes.Buffer))
	command.SetArgs([]string{"--server", server.URL, "--token", "gsk_test", "notifications", "history", "--channel-id", "channel-1", "--job-id", "job-1", "--status", "dead", "--limit", "25", "--cursor", "next-page"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
}
