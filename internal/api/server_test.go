package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

func TestRoutesDoNotServeWebUI(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	NewServer(nil, nil).Routes().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestRoutesUseConfiguredContextPath(t *testing.T) {
	server := NewServer(nil, nil)
	server.SetContextPath("/scheduler")
	handler := server.Routes()

	for _, test := range []struct {
		name, target string
		want         int
	}{
		{name: "prefixed health endpoint", target: "/scheduler/health/live", want: http.StatusOK},
		{name: "unprefixed endpoint is hidden", target: "/health/live", want: http.StatusNotFound},
		{name: "context root does not serve UI", target: "/scheduler/", want: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.target, nil))
			if response.Code != test.want {
				t.Fatalf("GET %s status = %d, want %d", test.target, response.Code, test.want)
			}
		})
	}
}

func TestRequestLogIncludesResponseStatus(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	for _, test := range []struct {
		name string
		code int
	}{
		{name: "success", code: http.StatusNoContent},
		{name: "client error", code: http.StatusTeapot},
		{name: "server error", code: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
			handler := NewServer(nil, nil).requestLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.code)
			}))
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/probe", nil))

			line := output.String()
			for _, want := range []string{`"msg":"http request"`, `"method":"GET"`, `"path":"/probe"`, `"status":` + strconv.Itoa(test.code)} {
				if !strings.Contains(line, want) {
					t.Errorf("log output %q does not contain %q", line, want)
				}
			}
		})
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

func TestDecodeStandardJSONRejectsTrailingDocument(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/example", strings.NewReader(`{"name":"first"}{"name":"second"}`))
	response := httptest.NewRecorder()
	var body struct {
		Name string `json:"name"`
	}
	if decode(response, request, &body) {
		t.Fatal("decode unexpectedly accepted a trailing JSON document")
	}
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestApplyDefaultsSetsHTTPHandler(t *testing.T) {
	t.Parallel()
	job := &schedulerv1.Job{TargetUrl: "https://example.com/hook"}
	applyDefaults(job)
	if job.ExecutorHandler != "__http__" {
		t.Fatalf("handler = %q, want __http__", job.ExecutorHandler)
	}
	script := &schedulerv1.Job{TargetUrl: "https://example.com/hook", ScriptLanguage: "shell"}
	applyDefaults(script)
	if script.ExecutorHandler != "" {
		t.Fatalf("script job handler = %q, want empty", script.ExecutorHandler)
	}
}

func TestKubernetesClusterCapacityRoundTripsThroughAPIModel(t *testing.T) {
	t.Parallel()
	cluster := clusterFromRequest("tenant", "cluster", kubernetesClusterRequest{Name: "production", MaxConcurrentJobs: 250})
	if cluster.MaxConcurrentJobs != 250 {
		t.Fatalf("store cluster capacity = %d", cluster.MaxConcurrentJobs)
	}
	public := publicKubernetesCluster(&schedulerv1.KubernetesCluster{Id: "cluster", MaxConcurrentJobs: 250})
	if public["max_concurrent_jobs"] != int32(250) {
		t.Fatalf("public cluster capacity = %#v", public["max_concurrent_jobs"])
	}
}
