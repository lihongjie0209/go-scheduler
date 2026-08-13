package scriptexecutor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/pkg/executor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func Run() error {
	schedulerAddress := os.Getenv("SCHEDULER_GRPC_ADDRESS")
	token, groupID := os.Getenv("SCHEDULER_TOKEN"), os.Getenv("EXECUTOR_GROUP_ID")
	nodeID, address := os.Getenv("EXECUTOR_NODE_ID"), os.Getenv("EXECUTOR_ADVERTISE_ADDRESS")
	tenantID := envOr("EXECUTOR_TENANT_ID", "00000000-0000-0000-0000-000000000001")
	listen := envOr("EXECUTOR_LISTEN", ":9999")
	if schedulerAddress == "" || token == "" || groupID == "" || nodeID == "" || address == "" {
		return errors.New("SCHEDULER_GRPC_ADDRESS, SCHEDULER_TOKEN, EXECUTOR_GROUP_ID, EXECUTOR_NODE_ID and EXECUTOR_ADVERTISE_ADDRESS are required")
	}
	languages := splitLanguages(envOr("SCRIPT_LANGUAGES", "shell,python,nodejs,php,powershell"))
	labels := splitLanguages(os.Getenv("EXECUTOR_LABELS"))
	server, err := executor.NewServer(executor.Options{SchedulerURL: "http://scheduler.invalid"})
	if err != nil {
		return err
	}
	if err = server.Handle("__script__", executor.ScriptHandler(executor.ScriptOptions{Languages: languages})); err != nil {
		return err
	}
	if err = server.Handle("__http__", executor.HTTPHandler(nil)); err != nil {
		return err
	}
	if dockerEnabled := envOr("DOCKER_ENABLED", "false"); dockerEnabled == "true" {
		if err = server.Handle("__docker__", executor.DockerHandler(executor.DockerOptions{Binary: envOr("DOCKER_BINARY", "docker")})); err != nil {
			return err
		}
	} else if dockerEnabled != "false" {
		return errors.New("DOCKER_ENABLED must be true or false")
	}
	if kubernetesEnabled := envOr("KUBERNETES_ENABLED", "false"); kubernetesEnabled == "true" {
		if err = server.HandleAsync("__kubernetes__", executor.KubernetesHandler(executor.KubernetesOptions{})); err != nil {
			return err
		}
	} else if kubernetesEnabled != "false" {
		return errors.New("KUBERNETES_ENABLED must be true or false")
	}
	ttl, err := time.ParseDuration(envOr("EXECUTOR_TTL", "30s"))
	if err != nil {
		return fmt.Errorf("parse EXECUTOR_TTL: %w", err)
	}
	connection, err := grpc.NewClient(schedulerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(token)))
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	schedulerClient := schedulerv1.NewSchedulerServiceClient(connection)
	reporter, err := executor.NewGRPCReporter(schedulerClient)
	if err != nil {
		return err
	}
	executorService, err := executor.NewGRPCServer(server, reporter)
	if err != nil {
		return err
	}
	registrar, err := executor.NewGRPCRegistrar(schedulerClient, executor.GRPCRegistrarOptions{TenantID: tenantID, GroupID: groupID, NodeID: nodeID, Address: address, TTL: ttl, Labels: labels})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", listen)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(rpc.UnaryRecovery(), rpc.UnaryLogging(), rpc.UnaryServerAuth(token, "")))
	executorv1.RegisterExecutorServiceServer(grpcServer, executorService)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()
	registrarErr := make(chan error, 1)
	go func() { registrarErr <- registrar.Run(ctx) }()
	select {
	case <-ctx.Done():
	case err = <-serverErr:
		if err != nil {
			stop()
		}
	case err = <-registrarErr:
		if err != nil {
			stop()
		}
	}
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpcServer.GracefulStop()
	if err != nil {
		return err
	}
	return nil
}
func splitLanguages(raw string) []string {
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if value := strings.TrimSpace(field); value != "" {
			out = append(out, value)
		}
	}
	return out
}
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
