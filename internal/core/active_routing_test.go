package core

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	pkgexecutor "github.com/lihongjie0209/go-scheduler/pkg/executor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, nil, "failover", "job-1", []executorCandidate{{ID: "a", Address: unhealthy.URL}, {ID: "b", Address: healthy.URL}}, 200*time.Millisecond)
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
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, nil, "busyover", "job-9", []executorCandidate{{ID: "a", Address: busy.URL}, {ID: "b", Address: idle.URL}}, 200*time.Millisecond)
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
	if _, err := selectActiveExecutor(ctx, &http.Client{}, nil, "failover", "job", []executorCandidate{{ID: "a", Address: blocked.URL}}, time.Second); err == nil {
		t.Fatal("expected unavailable executor error")
	}
}

func TestExecutorAddressUsesGRPC(t *testing.T) {
	t.Parallel()
	tests := []struct {
		address string
		want    bool
	}{
		{address: "127.0.0.1:9999", want: true},
		{address: "grpc://worker:9999", want: true},
		{address: "grpcs://worker:9999", want: true},
		{address: "http://worker:9999", want: false},
		{address: "https://worker:9999", want: false},
		{address: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			t.Parallel()
			if got := executorAddressUsesGRPC(tt.address); got != tt.want {
				t.Fatalf("executorAddressUsesGRPC(%q) = %v, want %v", tt.address, got, tt.want)
			}
		})
	}
}

type silentReporter struct{}

func (silentReporter) AppendLog(context.Context, string, string, string, string) error {
	return nil
}
func (silentReporter) Complete(context.Context, string, string, bool, string) error { return nil }

func startProbeGRPCExecutor(t *testing.T, serving bool, busyJob string) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	healthServer := health.NewServer()
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthpb.HealthCheckResponse_SERVING
	}
	healthServer.SetServingStatus("", status)
	healthpb.RegisterHealthServer(server, healthServer)
	executorServer, err := pkgexecutor.NewServer(pkgexecutor.Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	if busyJob != "" {
		if err = executorServer.Handle("busy", func(ctx context.Context, _ pkgexecutor.Task) error {
			<-ctx.Done()
			return ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	}
	grpcService, err := pkgexecutor.NewGRPCServer(executorServer, silentReporter{})
	if err != nil {
		t.Fatal(err)
	}
	if busyJob != "" {
		if _, err = grpcService.Dispatch(t.Context(), &executorv1.DispatchRequest{RunId: "busy-run", JobId: busyJob, Handler: "busy", CallbackToken: "token", TimeoutSeconds: 30}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for {
			state, inspectErr := grpcService.Inspect(t.Context(), &executorv1.InspectRequest{JobId: busyJob})
			if inspectErr == nil && state.GetState() == "busy" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("busy job never became inspectable: %v %+v", inspectErr, state)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	executorv1.RegisterExecutorServiceServer(server, grpcService)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func TestSelectActiveExecutorGRPCFailoverSkipsUnhealthyNode(t *testing.T) {
	t.Parallel()
	unhealthy := startProbeGRPCExecutor(t, false, "")
	healthy := startProbeGRPCExecutor(t, true, "")
	pool := newExecutorGRPCPoolWithTransport("token", insecure.NewCredentials())
	t.Cleanup(pool.close)
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, pool, "failover", "job-1", []executorCandidate{{ID: "a", Address: "grpc://" + unhealthy}, {ID: "b", Address: "grpc://" + healthy}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "b" {
		t.Fatalf("node = %q, want b", node.ID)
	}
}

func TestSelectActiveExecutorGRPCBusyoverSkipsBusyNode(t *testing.T) {
	t.Parallel()
	busy := startProbeGRPCExecutor(t, true, "job-9")
	idle := startProbeGRPCExecutor(t, true, "")
	pool := newExecutorGRPCPoolWithTransport("token", insecure.NewCredentials())
	t.Cleanup(pool.close)
	node, err := selectActiveExecutor(t.Context(), &http.Client{}, pool, "busyover", "job-9", []executorCandidate{{ID: "a", Address: busy}, {ID: "b", Address: idle}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "b" {
		t.Fatalf("node = %q, want b", node.ID)
	}
}
