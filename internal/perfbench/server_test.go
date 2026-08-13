package perfbench

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServerLifecycle(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	server := httptest.NewServer(NewServer(collector))
	t.Cleanup(server.Close)
	base := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	expectBody, err := json.Marshal(map[string]any{"events": []ExpectedEvent{{ID: "event-1", ScheduledAt: base}}})
	if err != nil {
		t.Fatal(err)
	}
	response := request(t, http.MethodPost, server.URL+"/api/v1/expect", expectBody)
	status := response.StatusCode
	closeResponse(t, response)
	if status != http.StatusOK {
		t.Fatalf("expect status = %d", status)
	}

	response = request(t, http.MethodPost, server.URL+"/execute?id=event-1", nil)
	status = response.StatusCode
	closeResponse(t, response)
	if status != http.StatusOK {
		t.Fatalf("execute status = %d", status)
	}
	response = request(t, http.MethodPost, server.URL+"/execute?id=event-1", nil)
	closeResponse(t, response)

	response = request(t, http.MethodGet, server.URL+"/api/v1/report", nil)
	var report Report
	err = json.NewDecoder(response.Body).Decode(&report)
	closeResponse(t, response)
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected != 1 || report.Received != 1 || report.Missing != 0 || report.DuplicateRequests != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestServerRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	collector := NewCollector()
	server := httptest.NewServer(NewServer(collector))
	t.Cleanup(server.Close)
	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
		status int
	}{
		{name: "missing execution ID", method: http.MethodPost, path: "/execute", status: http.StatusBadRequest},
		{name: "malformed expectation", method: http.MethodPost, path: "/api/v1/expect", body: []byte(`{"events":`), status: http.StatusBadRequest},
		{name: "unknown expectation field", method: http.MethodPost, path: "/api/v1/expect", body: []byte(`{"events":[],"unknown":true}`), status: http.StatusBadRequest},
		{name: "oversized execution body", method: http.MethodPost, path: "/execute?id=large", body: bytes.Repeat([]byte{'x'}, maxExecutionBody+1), status: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := request(t, test.method, server.URL+test.path, test.body)
			status := response.StatusCode
			closeResponse(t, response)
			if status != test.status {
				t.Fatalf("status = %d, want %d", status, test.status)
			}
		})
	}
	if report := collector.Report(); report.InvalidRequests != 4 {
		t.Fatalf("invalid requests = %d, want 4", report.InvalidRequests)
	}
}

func request(t *testing.T, method, target string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func closeResponse(t *testing.T, response *http.Response) {
	t.Helper()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Errorf("drain response body: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}
