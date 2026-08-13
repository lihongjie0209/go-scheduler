package core

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type recordingCancellationServer struct {
	executorv1.UnimplementedExecutorServiceServer
	request chan *executorv1.CancelRequest
}

func (s *recordingCancellationServer) Cancel(_ context.Context, request *executorv1.CancelRequest) (*executorv1.CancelResponse, error) {
	s.request <- request
	return &executorv1.CancelResponse{Accepted: true}, nil
}

func TestExecutorControllerCancel(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	recorder := &recordingCancellationServer{request: make(chan *executorv1.CancelRequest, 1)}
	executorv1.RegisterExecutorServiceServer(server, recorder)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	controller := NewExecutorController("token", insecure.NewCredentials())
	t.Cleanup(controller.Close)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err = controller.Cancel(ctx, listener.Addr().String(), "run-1", "operator requested"); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-recorder.request:
		if request.GetRunId() != "run-1" || request.GetReason() != "operator requested" {
			t.Fatalf("cancel request = %+v", request)
		}
	case <-ctx.Done():
		t.Fatal("executor did not receive cancel request")
	}
}

func TestExecutorGRPCPoolEvictsIdleConnections(t *testing.T) {
	t.Parallel()
	pool := newExecutorGRPCPool("token")
	defer pool.close()
	for index := range maxExecutorGRPCConnections + 20 {
		_, release, err := pool.acquire(fmt.Sprintf("127.0.0.1:%d", 10000+index))
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if got := len(pool.conns); got != maxExecutorGRPCConnections {
		t.Fatalf("connection count = %d, want %d", got, maxExecutorGRPCConnections)
	}
}

func TestExecutorGRPCPoolDoesNotEvictActiveConnections(t *testing.T) {
	t.Parallel()
	pool := newExecutorGRPCPool("token")
	defer pool.close()
	releases := make([]func(), 0, maxExecutorGRPCConnections)
	for index := range maxExecutorGRPCConnections {
		_, release, err := pool.acquire(fmt.Sprintf("127.0.0.1:%d", 10000+index))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, _, err := pool.acquire("127.0.0.1:65535"); err == nil {
		t.Fatal("pool accepted a connection while all entries were active")
	}
	for _, release := range releases {
		release()
	}
}
