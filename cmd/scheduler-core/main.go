package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/notifier"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	fx.New(fx.Provide(loadConfig, newStore, newEtcd, newExecutorRegistry, newCoreService, newGRPCServer, newRegistrar, newEngine, newNotifier, newCoreHTTPServer), fx.Invoke(registerDatabasePoolMetrics, run)).Run()
}
func loadConfig() (config.Config, error) { return config.Load("scheduler-core") }
func newStore(lc fx.Lifecycle, c config.Config) (*store.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ring, err := cryptox.NewKeyring(c.MasterKeyVersion, c.MasterKey)
	if err != nil {
		return nil, err
	}
	s, err := store.New(ctx, c.DatabaseURL, store.WithHeaderCipher(ring), store.WithPoolSize(int32(c.CoreDatabaseMaxConns), int32(c.CoreDatabaseMinConns)))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { s.Close(); return nil }})
	return s, nil
}
func registerDatabasePoolMetrics(s *store.Store) error {
	return prometheus.Register(observability.NewDatabasePoolCollector("core", s.PoolStats))
}
func newEtcd(lc fx.Lifecycle, c config.Config) (*clientv3.Client, error) {
	client, err := discovery.NewClient(c.EtcdEndpoints, c.EtcdUsername, c.EtcdPassword, c.EtcdCA, c.EtcdCert, c.EtcdKey)
	if err == nil {
		lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	}
	return client, err
}
func newExecutorRegistry(c config.Config, client *clientv3.Client, s *store.Store) core.ExecutorRegistry {
	return discovery.NewExecutorRegistry(client, c.EtcdPrefix, s)
}
func newCoreService(s *store.Store, registry core.ExecutorRegistry) *core.Service {
	return core.NewService(s, registry)
}
func newGRPCServer(c config.Config, svc *core.Service) (*grpc.Server, error) {
	opts := []grpc.ServerOption{grpc.ChainUnaryInterceptor(rpc.UnaryRecovery(), rpc.UnaryLogging(), rpc.UnaryServerAuth(c.ServiceToken, c.PreviousToken))}
	if c.GRPCTLSCert != "" || c.GRPCTLSKey != "" {
		if c.GRPCTLSCert == "" || c.GRPCTLSKey == "" {
			return nil, fmt.Errorf("GRPC_TLS_CERT and GRPC_TLS_KEY must be configured together")
		}
		creds, err := credentials.NewServerTLSFromFile(c.GRPCTLSCert, c.GRPCTLSKey)
		if err != nil {
			return nil, fmt.Errorf("load grpc server TLS: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	server := grpc.NewServer(opts...)
	schedulerv1.RegisterSchedulerServiceServer(server, svc)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return server, nil
}
func newRegistrar(c config.Config, client *clientv3.Client) (*discovery.Registrar, error) {
	return discovery.NewRegistrar(client, c.EtcdPrefix, "scheduler-core", discovery.Metadata{InstanceID: c.InstanceID, GRPCAddress: c.AdvertiseGRPC, Version: "dev", StartedAt: time.Now().UTC()})
}
func newEngine(c config.Config, s *store.Store) *core.Engine {
	return core.NewEngine(s, c.InstanceID, c.SchedulerInterval, c.Workers, c.PublicBaseURL, c.HistoryRetention, c.TargetAllowlist)
}
func newNotifier(c config.Config, s *store.Store) *notifier.Worker {
	return notifier.New(s, c.InstanceID, notifier.SMTPConfig{Address: c.SMTPAddress, Username: c.SMTPUsername, Password: c.SMTPPassword, From: c.SMTPFrom})
}
func newCoreHTTPServer(c config.Config, s *store.Store) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return &http.Server{Addr: c.CoreHTTPAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
}
func run(lc fx.Lifecycle, c config.Config, server *grpc.Server, registrar *discovery.Registrar, engine *core.Engine, notifications *notifier.Worker, adminServer *http.Server) {
	var listener net.Listener
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		var err error
		listener, err = (&net.ListenConfig{}).Listen(ctx, "tcp", c.GRPCAddress)
		if err != nil {
			return fmt.Errorf("listen grpc: %w", err)
		}
		runCtx, stop := context.WithCancel(context.Background())
		cancel = stop
		go func() {
			if err := registrar.Run(runCtx); err != nil && runCtx.Err() == nil {
				slog.Error("registrar stopped", "error", err)
			}
		}()
		go func() {
			if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("core admin server stopped", "error", err)
			}
		}()
		go func() {
			if err := server.Serve(listener); err != nil {
				slog.Error("grpc server stopped", "error", err)
			}
		}()
		engine.Run(runCtx)
		notifications.Run(runCtx)
		return nil
	}, OnStop: func(ctx context.Context) error {
		cancel()
		revokeCtx, revokeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer revokeCancel()
		_ = registrar.Close(revokeCtx)
		stopped := make(chan struct{})
		go func() { server.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-ctx.Done():
			server.Stop()
		}
		_ = adminServer.Shutdown(ctx)
		engine.Wait()
		notifications.Wait()
		return nil
	}})
}
