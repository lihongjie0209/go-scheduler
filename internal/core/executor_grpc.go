package core

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type executorGRPCPool struct {
	token string
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newExecutorGRPCPool(token string) *executorGRPCPool {
	return &executorGRPCPool{token: token, conns: make(map[string]*grpc.ClientConn)}
}

func (p *executorGRPCPool) client(address string) (executorv1.ExecutorServiceClient, error) {
	target, err := executorGRPCTarget(address)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if connection := p.conns[target]; connection != nil {
		return executorv1.NewExecutorServiceClient(connection), nil
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(p.token)))
	if err != nil {
		return nil, fmt.Errorf("connect executor %q: %w", target, err)
	}
	p.conns[target] = connection
	return executorv1.NewExecutorServiceClient(connection), nil
}

func (p *executorGRPCPool) dispatch(ctx context.Context, address string, request *executorv1.DispatchRequest) error {
	client, err := p.client(address)
	if err != nil {
		return err
	}
	response, err := client.Dispatch(ctx, request)
	if err != nil {
		return fmt.Errorf("dispatch executor: %w", err)
	}
	if !response.GetAccepted() {
		return fmt.Errorf("executor rejected run")
	}
	return nil
}

func (p *executorGRPCPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for target, connection := range p.conns {
		_ = connection.Close()
		delete(p.conns, target)
	}
}

func executorGRPCTarget(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", fmt.Errorf("executor address is empty")
	}
	if !strings.Contains(address, "://") {
		return address, nil
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Host == "" || parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("invalid executor gRPC address %q", address)
	}
	return parsed.Host, nil
}
