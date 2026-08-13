package core

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type executorGRPCPool struct {
	token     string
	transport credentials.TransportCredentials
	mu        sync.Mutex
	conns     map[string]*executorGRPCConnection
}

type executorGRPCConnection struct {
	connection *grpc.ClientConn
	lastUsed   time.Time
	inUse      int
}

const maxExecutorGRPCConnections = 256

func newExecutorGRPCPool(token string) *executorGRPCPool {
	return newExecutorGRPCPoolWithTransport(token, insecure.NewCredentials())
}

func newExecutorGRPCPoolWithTransport(token string, transport credentials.TransportCredentials) *executorGRPCPool {
	return &executorGRPCPool{token: token, transport: transport, conns: make(map[string]*executorGRPCConnection)}
}

func (p *executorGRPCPool) acquire(address string) (executorv1.ExecutorServiceClient, func(), error) {
	target, err := executorGRPCTarget(address)
	if err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	if entry := p.conns[target]; entry != nil {
		entry.inUse++
		entry.lastUsed = time.Now()
		p.mu.Unlock()
		return executorv1.NewExecutorServiceClient(entry.connection), p.releaseFunc(target, entry), nil
	}
	if len(p.conns) >= maxExecutorGRPCConnections && !p.evictIdleLocked() {
		p.mu.Unlock()
		return nil, nil, fmt.Errorf("executor gRPC connection pool is full")
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(p.transport.Clone()), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(p.token)))
	if err != nil {
		p.mu.Unlock()
		return nil, nil, fmt.Errorf("connect executor %q: %w", target, err)
	}
	entry := &executorGRPCConnection{connection: connection, lastUsed: time.Now(), inUse: 1}
	p.conns[target] = entry
	p.mu.Unlock()
	return executorv1.NewExecutorServiceClient(connection), p.releaseFunc(target, entry), nil
}

func (p *executorGRPCPool) releaseFunc(target string, expected *executorGRPCConnection) func() {
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		entry := p.conns[target]
		if entry != expected || entry.inUse == 0 {
			return
		}
		entry.inUse--
		entry.lastUsed = time.Now()
	}
}

func (p *executorGRPCPool) evictIdleLocked() bool {
	var oldestTarget string
	var oldest *executorGRPCConnection
	for target, entry := range p.conns {
		if entry.inUse != 0 || oldest != nil && !entry.lastUsed.Before(oldest.lastUsed) {
			continue
		}
		oldestTarget, oldest = target, entry
	}
	if oldest == nil {
		return false
	}
	delete(p.conns, oldestTarget)
	_ = oldest.connection.Close()
	return true
}

func (p *executorGRPCPool) dispatch(ctx context.Context, address string, request *executorv1.DispatchRequest) error {
	client, release, err := p.acquire(address)
	if err != nil {
		return err
	}
	defer release()
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
	for target, entry := range p.conns {
		_ = entry.connection.Close()
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
