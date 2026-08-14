package scriptexecutor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	executorv1 "github.com/lihongjie0209/go-scheduler/gen/executor/v1"
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/pkg/executor"
	"google.golang.org/grpc"
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
		dockerOptions := executor.DockerOptions{Binary: envOr("DOCKER_BINARY", "docker")}
		if err = server.Handle("__docker__", executor.DockerHandler(dockerOptions)); err != nil {
			return err
		}
		if err = server.HandleExternalCancellation("docker", executor.DockerCanceller(dockerOptions)); err != nil {
			return err
		}
	} else if dockerEnabled != "false" {
		return errors.New("DOCKER_ENABLED must be true or false")
	}
	if kubernetesEnabled := envOr("KUBERNETES_ENABLED", "false"); kubernetesEnabled == "true" {
		kubernetesOptions := executor.KubernetesOptions{}
		if err = server.HandleAsync("__kubernetes__", executor.KubernetesHandler(kubernetesOptions)); err != nil {
			return err
		}
		if err = server.HandleExternalCancellation("kubernetes", executor.KubernetesCanceller(kubernetesOptions)); err != nil {
			return err
		}
	} else if kubernetesEnabled != "false" {
		return errors.New("KUBERNETES_ENABLED must be true or false")
	}
	ttl, err := time.ParseDuration(envOr("EXECUTOR_TTL", "30s"))
	if err != nil {
		return fmt.Errorf("parse EXECUTOR_TTL: %w", err)
	}
	maxConcurrency, err := positiveIntEnv("EXECUTOR_MAX_CONCURRENCY", 32)
	if err != nil {
		return err
	}
	completionMaxPending, err := positiveIntEnv("EXECUTOR_COMPLETION_MAX_PENDING", 10_000)
	if err != nil {
		return err
	}
	completionStore, err := executor.NewFileCompletionStore(envOr("EXECUTOR_STATE_DIR", "./executor-state"), executor.FileCompletionStoreOptions{MaxRecords: completionMaxPending})
	if err != nil {
		return err
	}
	shutdownTimeout, err := durationEnv("EXECUTOR_SHUTDOWN_TIMEOUT", 30*time.Second, time.Second, 24*time.Hour)
	if err != nil {
		return err
	}
	schedulerTransport, err := rpc.ClientTransportCredentials(os.Getenv("SCHEDULER_GRPC_TLS_CA"), os.Getenv("SCHEDULER_GRPC_TLS_SERVER_NAME"))
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(schedulerAddress, grpc.WithTransportCredentials(schedulerTransport), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(token)))
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	schedulerClient := schedulerv1.NewSchedulerServiceClient(connection)
	reporter, err := executor.NewGRPCReporter(schedulerClient)
	if err != nil {
		return err
	}
	executorService, err := executor.NewGRPCServer(server, reporter, executor.GRPCServerOptions{MaxConcurrentExecutions: maxConcurrency, CompletionStore: completionStore})
	if err != nil {
		return err
	}
	registrar, err := executor.NewGRPCRegistrar(schedulerClient, executor.GRPCRegistrarOptions{TenantID: tenantID, GroupID: groupID, NodeID: nodeID, Address: address, TTL: ttl, Labels: labels})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	deliveryCtx, cancelDelivery := context.WithCancel(context.Background())
	defer func() {
		cancelDelivery()
		executorService.WaitCompletionDelivery()
	}()
	executorService.RunCompletionDelivery(deliveryCtx)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", listen)
	if err != nil {
		return err
	}
	serverOptions := []grpc.ServerOption{grpc.ChainUnaryInterceptor(rpc.UnaryRecovery(), rpc.UnaryLogging(), rpc.UnaryServerAuth(token, ""))}
	executorTransport, err := rpc.ServerTransportCredentials(os.Getenv("EXECUTOR_GRPC_TLS_CERT"), os.Getenv("EXECUTOR_GRPC_TLS_KEY"))
	if err != nil {
		return err
	}
	if executorTransport != nil {
		serverOptions = append(serverOptions, grpc.Creds(executorTransport))
	}
	grpcServer := grpc.NewServer(serverOptions...)
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
	registrarStopped := false
	select {
	case <-ctx.Done():
	case err = <-serverErr:
		if err != nil {
			stop()
		}
	case err = <-registrarErr:
		registrarStopped = true
		if err != nil {
			stop()
		}
	}
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	stop()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	drainErr := executorService.Drain(shutdownCtx)
	cancelShutdown()
	cancelDelivery()
	executorService.WaitCompletionDelivery()
	grpcServer.GracefulStop()
	if err != nil {
		return err
	}
	if drainErr != nil && !errors.Is(drainErr, context.DeadlineExceeded) {
		return drainErr
	}
	if !registrarStopped {
		select {
		case registrarErrValue := <-registrarErr:
			if registrarErrValue != nil {
				return registrarErrValue
			}
		case <-time.After(5 * time.Second):
		}
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

func positiveIntEnv(key string, fallback int) (int, error) {
	value := envOr(key, strconv.Itoa(fallback))
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}

func durationEnv(key string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	value := envOr(key, fallback.String())
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", key, minimum, maximum)
	}
	return parsed, nil
}
