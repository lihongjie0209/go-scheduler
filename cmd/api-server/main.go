package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	fx.New(fx.Provide(loadConfig, newStore, newEtcd, newCoreClient, newRegistrar, newAuthManager, newHTTPServer), fx.Invoke(run)).Run()
}
func loadConfig() (config.Config, error) { return config.Load("api-server") }
func newStore(lc fx.Lifecycle, c config.Config) (*store.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ring, err := cryptox.NewKeyring(c.MasterKeyVersion, c.MasterKey)
	if err != nil {
		return nil, err
	}
	s, err := store.New(ctx, c.DatabaseURL, store.WithHeaderCipher(ring))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { s.Close(); return nil }})
	return s, nil
}
func newEtcd(lc fx.Lifecycle, c config.Config) (*clientv3.Client, error) {
	client, err := discovery.NewClient(c.EtcdEndpoints, c.EtcdUsername, c.EtcdPassword, c.EtcdCA, c.EtcdCert, c.EtcdKey)
	if err == nil {
		lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	}
	return client, err
}
func newCoreClient(lc fx.Lifecycle, c config.Config, etcd *clientv3.Client) (schedulerv1.SchedulerServiceClient, error) {
	builder := discovery.NewBuilder(etcd, c.EtcdPrefix)
	transport := credentials.TransportCredentials(insecure.NewCredentials())
	if c.GRPCTLSCA != "" {
		creds, err := credentials.NewClientTLSFromFile(c.GRPCTLSCA, c.GRPCTLSServerName)
		if err != nil {
			return nil, fmt.Errorf("load grpc client TLS: %w", err)
		}
		transport = creds
	}
	conn, err := grpc.NewClient("etcd:///scheduler-core", grpc.WithResolvers(builder), grpc.WithTransportCredentials(transport), grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, roundrobin.Name)), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(c.ServiceToken)))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return conn.Close() }})
	return schedulerv1.NewSchedulerServiceClient(conn), nil
}
func newRegistrar(c config.Config, client *clientv3.Client) (*discovery.Registrar, error) {
	return discovery.NewRegistrar(client, c.EtcdPrefix, "api-server", discovery.Metadata{InstanceID: c.InstanceID, HTTPAddress: c.AdvertiseHTTP, Version: "dev", StartedAt: time.Now().UTC()})
}
func newAuthManager(c config.Config) (*auth.Manager, error) {
	return auth.NewManager(c.JWTSecret, "go-scheduler", 15*time.Minute)
}
func newHTTPServer(c config.Config, client schedulerv1.SchedulerServiceClient, s *store.Store, manager *auth.Manager, etcd *clientv3.Client) *http.Server {
	handler := apihttp.NewServer(client, s, manager, c.CookieSecure)
	handler.SetContextPath(c.APIContextPath)
	handler.SetDiscovery(etcd, c.EtcdPrefix)
	return &http.Server{Addr: c.HTTPAddress, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
}
func run(lc fx.Lifecycle, server *http.Server, registrar *discovery.Registrar) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		ctx, stop := context.WithCancel(context.Background())
		cancel = stop
		go func() {
			if err := registrar.Run(ctx); err != nil && ctx.Err() == nil {
				slog.Error("registrar stopped", "error", err)
			}
		}()
		go func() {
			slog.Info("api server listening", "address", server.Addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http server stopped", "error", err)
			}
		}()
		return nil
	}, OnStop: func(ctx context.Context) error {
		cancel()
		_ = registrar.Close(ctx)
		return server.Shutdown(ctx)
	}})
}
