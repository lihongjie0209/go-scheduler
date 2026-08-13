package rpc

import (
	"context"
	"fmt"
	"net"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const inProcessBufferSize = 1024 * 1024

// InProcessScheduler runs the Scheduler gRPC transport entirely in memory.
// It preserves the API/Core protocol boundary without requiring discovery or
// exposing a network listener.
type InProcessScheduler struct {
	client   schedulerv1.SchedulerServiceClient
	conn     *grpc.ClientConn
	server   *grpc.Server
	listener *bufconn.Listener
}

func NewInProcessScheduler(service schedulerv1.SchedulerServiceServer, serviceToken, previousToken string) (*InProcessScheduler, error) {
	listener := bufconn.Listen(inProcessBufferSize)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		UnaryRecovery(),
		UnaryLogging(),
		UnaryServerAuth(serviceToken, previousToken),
	))
	schedulerv1.RegisterSchedulerServiceServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///scheduler-core-in-process",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithUnaryInterceptor(UnaryClientAuth(serviceToken)),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		return nil, fmt.Errorf("create in-process scheduler client: %w", err)
	}
	return &InProcessScheduler{client: schedulerv1.NewSchedulerServiceClient(conn), conn: conn, server: server, listener: listener}, nil
}

func (s *InProcessScheduler) Client() schedulerv1.SchedulerServiceClient { return s.client }

func (s *InProcessScheduler) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.server.Stop()
	}
	connErr := s.conn.Close()
	listenerErr := s.listener.Close()
	if connErr != nil {
		return connErr
	}
	return listenerErr
}
