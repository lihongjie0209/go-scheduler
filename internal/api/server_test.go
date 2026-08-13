package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

func TestRoutesDoNotServeWebUI(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	NewServer(nil, nil, nil).Routes().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestDecodeProtobufJSONAcceptsStringInt64(t *testing.T) {
	request := httptest.NewRequest("PUT", "/api/v1/jobs/job-1", strings.NewReader(`{"name":"updated","version":"7"}`))
	response := httptest.NewRecorder()
	var job schedulerv1.Job
	if !decode(response, request, &job) {
		t.Fatalf("decode failed: %s", response.Body.String())
	}
	if job.Name != "updated" || job.Version != 7 {
		t.Fatalf("job = %+v", &job)
	}
}

func TestDecodeProtobufJSONRejectsUnknownFields(t *testing.T) {
	request := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(`{"name":"job","unknown":true}`))
	response := httptest.NewRecorder()
	var job schedulerv1.Job
	if decode(response, request, &job) {
		t.Fatal("decode unexpectedly accepted an unknown field")
	}
	if response.Code != 400 {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}
