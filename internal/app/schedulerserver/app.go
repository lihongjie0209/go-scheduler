package schedulerserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	appRuntime "github.com/lihongjie0209/go-scheduler/internal/app/runtime"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/notifier"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func Run() {
	fx.New(Options()...).Run()
}

func Options() []fx.Option {
	return []fx.Option{
		appRuntime.LoggingOption(),
		fx.Provide(
			loadConfig,
			newStoreCipher,
			newCoreStore,
			newExecutorController,
			newCoreService,
			newGRPCServer,
			newInProcessScheduler,
			newCoreClient,
			appRuntime.NewAuthManager,
			newHTTPServer,
			newEngine,
			newNotifier,
		),
		fx.Invoke(registerDatabasePoolMetrics, run),
	}
}

func loadConfig() (config.Config, error) { return config.Load("scheduler-server") }

type coreStore struct{ *store.Store }

func newStoreCipher(c config.Config) (store.HeaderCipher, error) {
	return cryptox.NewKeyring(c.MasterKeyVersion, c.MasterKey)
}

func openStore(lc fx.Lifecycle, c config.Config, cipher store.HeaderCipher, maxConns, minConns int32) (*store.Store, error) {
	return appRuntime.OpenStore(lc, c.DatabaseURL, cipher, maxConns, minConns)
}

func newCoreStore(lc fx.Lifecycle, c config.Config, cipher store.HeaderCipher) (*coreStore, error) {
	s, err := openStore(lc, c, cipher, c.CoreDatabaseMaxConns, c.CoreDatabaseMinConns)
	if err != nil {
		return nil, err
	}
	return &coreStore{Store: s}, nil
}

func newExecutorController(c config.Config) (*core.ExecutorController, error) {
	transport, err := rpc.ClientTransportCredentials(c.ExecutorGRPCTLSCA, c.ExecutorGRPCTLSServerName)
	if err != nil {
		return nil, err
	}
	return core.NewExecutorController(c.ServiceToken, transport), nil
}

func newCoreService(s *coreStore, controller *core.ExecutorController) *core.Service {
	return core.NewServiceWithExecutorController(s.Store, s.Store, controller)
}

func newGRPCServer(c config.Config, service *core.Service) (*grpc.Server, error) {
	options := []grpc.ServerOption{grpc.ChainUnaryInterceptor(rpc.UnaryRecovery(), rpc.UnaryLogging(), rpc.UnaryServerAuth(c.ServiceToken, c.PreviousToken))}
	transport, err := rpc.ServerTransportCredentials(c.GRPCTLSCert, c.GRPCTLSKey)
	if err != nil {
		return nil, err
	}
	if transport != nil {
		options = append(options, grpc.Creds(transport))
	}
	server := grpc.NewServer(options...)
	schedulerv1.RegisterSchedulerServiceServer(server, service)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return server, nil
}

func registerDatabasePoolMetrics(core *coreStore) error {
	if err := prometheus.Register(observability.NewDatabasePoolCollector("core", core.PoolStats)); err != nil {
		return err
	}
	if err := prometheus.Register(observability.NewNotificationQueueCollector(func(ctx context.Context) (observability.NotificationQueueSnapshot, error) {
		stats, err := core.NotificationQueueStats(ctx)
		return observability.NotificationQueueSnapshot{Pending: stats.Pending, OldestPendingAge: stats.OldestPendingAge}, err
	})); err != nil {
		return err
	}
	return prometheus.Register(observability.NewExecutorCommandQueueCollector(func(ctx context.Context) (observability.ExecutorCommandQueueSnapshot, error) {
		stats, err := core.ExecutorCommandQueueStats(ctx)
		return observability.ExecutorCommandQueueSnapshot{Pending: stats.Pending, OldestPendingAge: stats.OldestPendingAge}, err
	}))
}

func newInProcessScheduler(lc fx.Lifecycle, c config.Config, service *core.Service) (*rpc.InProcessScheduler, error) {
	scheduler, err := rpc.NewInProcessScheduler(service, c.ServiceToken, c.PreviousToken)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: scheduler.Close})
	return scheduler, nil
}

func newCoreClient(scheduler *rpc.InProcessScheduler) schedulerv1.SchedulerServiceClient {
	return scheduler.Client()
}

func newHTTPServer(c config.Config, client schedulerv1.SchedulerServiceClient, manager *auth.Manager) *http.Server {
	handler := apihttp.NewServer(client, manager, c.CookieSecure)
	handler.SetContextPath(c.APIContextPath)
	handler.SetStandaloneInstance(c.InstanceID, time.Now())
	return appRuntime.NewHTTPServer(c.HTTPAddress, handler.Routes(), 15*time.Second, 30*time.Second)
}

func newEngine(c config.Config, s *coreStore, controller *core.ExecutorController) (*core.Engine, error) {
	return core.NewEngine(s.Store, c.InstanceID, c.SchedulerInterval, c.Workers, c.PublicBaseURL, c.HistoryRetention, nil, core.WithExecutorController(controller)), nil
}

func newNotifier(c config.Config, s *coreStore) *notifier.Worker {
	return notifier.New(s.Store, c.InstanceID, notifier.SMTPConfig{
		Address: c.SMTPAddress, Username: c.SMTPUsername, Password: c.SMTPPassword, From: c.SMTPFrom, TLSMode: c.SMTPTLSMode,
	})
}

func run(lc fx.Lifecycle, c config.Config, server *http.Server, grpcServer *grpc.Server, engine *core.Engine, notifications *notifier.Worker) {
	var cancel context.CancelFunc
	var grpcListener net.Listener
	var httpListener net.Listener
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var err error
			grpcListener, err = (&net.ListenConfig{}).Listen(ctx, "tcp", c.GRPCAddress)
			if err != nil {
				return fmt.Errorf("listen grpc: %w", err)
			}
			httpListener, err = (&net.ListenConfig{}).Listen(ctx, "tcp", server.Addr)
			if err != nil {
				_ = grpcListener.Close()
				return fmt.Errorf("listen http: %w", err)
			}
			runCtx, stop := context.WithCancel(context.Background())
			cancel = stop
			engine.Run(runCtx)
			notifications.Run(runCtx)
			go func() {
				slog.Info("scheduler server listening", "address", server.Addr, "mode", "standalone")
				if err := server.Serve(httpListener); err != nil && err != http.ErrServerClosed {
					slog.Error("scheduler server stopped", "error", err)
				}
			}()
			go func() {
				slog.Info("scheduler internal gRPC listening", "address", c.GRPCAddress, "mode", "standalone")
				if err := grpcServer.Serve(grpcListener); err != nil {
					slog.Error("scheduler internal gRPC stopped", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			grpcServer.GracefulStop()
			err := server.Shutdown(ctx)
			engine.Wait()
			notifications.Wait()
			return err
		},
	})
}
