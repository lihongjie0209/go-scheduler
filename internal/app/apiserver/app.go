package apiserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	appRuntime "github.com/lihongjie0209/go-scheduler/internal/app/runtime"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
)

func Run() {
	fx.New(Options()...).Run()
}

func Options() []fx.Option {
	return []fx.Option{
		appRuntime.LoggingOption(),
		fx.Provide(loadConfig, appRuntime.NewEtcd, newCoreClient, newRegistrar, appRuntime.NewAuthManager, newHTTPServer),
		fx.Invoke(run),
	}
}

func loadConfig() (config.Config, error) { return config.Load("api-server") }
func newCoreClient(lc fx.Lifecycle, c config.Config, etcd *clientv3.Client) (schedulerv1.SchedulerServiceClient, error) {
	transport, err := rpc.ClientTransportCredentials(c.GRPCTLSCA, c.GRPCTLSServerName)
	if err != nil {
		return nil, err
	}
	target := c.CoreGRPCTarget
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport), grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, roundrobin.Name)), grpc.WithUnaryInterceptor(rpc.UnaryClientAuth(c.ServiceToken))}
	if c.DiscoveryMode == "etcd" {
		if etcd == nil {
			return nil, fmt.Errorf("etcd client is required in etcd discovery mode")
		}
		target = "etcd:///scheduler-core"
		options = append(options, grpc.WithResolvers(discovery.NewBuilder(etcd, c.EtcdPrefix)))
	}
	conn, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return conn.Close() }})
	return schedulerv1.NewSchedulerServiceClient(conn), nil
}
func newRegistrar(c config.Config, client *clientv3.Client) (discovery.ServiceRegistrar, error) {
	if c.DiscoveryMode == "kubernetes" {
		return discovery.NewNoopRegistrar(), nil
	}
	return discovery.NewRegistrar(client, c.EtcdPrefix, "api-server", discovery.Metadata{InstanceID: c.InstanceID, HTTPAddress: c.AdvertiseHTTP, Version: "dev", StartedAt: time.Now().UTC()})
}
func newHTTPServer(c config.Config, client schedulerv1.SchedulerServiceClient, manager *auth.Manager, etcd *clientv3.Client) *http.Server {
	handler := apihttp.NewServer(client, manager, c.CookieSecure)
	handler.SetContextPath(c.APIContextPath)
	handler.SetDiscovery(etcd, c.EtcdPrefix)
	return appRuntime.NewHTTPServer(c.HTTPAddress, handler.Routes(), 15*time.Second, 30*time.Second)
}
func run(lc fx.Lifecycle, server *http.Server, registrar discovery.ServiceRegistrar) {
	var cancel context.CancelFunc
	var listener net.Listener
	lc.Append(fx.Hook{OnStart: func(startCtx context.Context) error {
		var err error
		listener, err = (&net.ListenConfig{}).Listen(startCtx, "tcp", server.Addr)
		if err != nil {
			return fmt.Errorf("listen http: %w", err)
		}
		ctx, stop := context.WithCancel(context.Background())
		cancel = stop
		go func() {
			if err := registrar.Run(ctx); err != nil && ctx.Err() == nil {
				slog.Error("registrar stopped", "error", err)
			}
		}()
		go func() {
			slog.Info("api server listening", "address", server.Addr)
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
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
