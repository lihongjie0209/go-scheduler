package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/notifier"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	fx.New(
		fx.Provide(
			loadConfig,
			newStoreCipher,
			newAPIStore,
			newCoreStore,
			newCoreService,
			newInProcessScheduler,
			newCoreClient,
			newAuthManager,
			newHTTPServer,
			newEngine,
			newNotifier,
		),
		fx.Invoke(registerDatabasePoolMetrics, run),
	).Run()
}

func loadConfig() (config.Config, error) { return config.Load("scheduler-server") }

type apiStore struct{ *store.Store }
type coreStore struct{ *store.Store }

func newStoreCipher(c config.Config) (store.HeaderCipher, error) {
	return cryptox.NewKeyring(c.MasterKeyVersion, c.MasterKey)
}

func openStore(lc fx.Lifecycle, c config.Config, cipher store.HeaderCipher, maxConns, minConns int) (*store.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.New(ctx, c.DatabaseURL, store.WithHeaderCipher(cipher), store.WithPoolSize(int32(maxConns), int32(minConns)))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		s.Close()
		return nil
	}})
	return s, nil
}

func newAPIStore(lc fx.Lifecycle, c config.Config, cipher store.HeaderCipher) (*apiStore, error) {
	s, err := openStore(lc, c, cipher, c.APIDatabaseMaxConns, c.APIDatabaseMinConns)
	if err != nil {
		return nil, err
	}
	return &apiStore{Store: s}, nil
}

func newCoreStore(lc fx.Lifecycle, c config.Config, cipher store.HeaderCipher) (*coreStore, error) {
	s, err := openStore(lc, c, cipher, c.CoreDatabaseMaxConns, c.CoreDatabaseMinConns)
	if err != nil {
		return nil, err
	}
	return &coreStore{Store: s}, nil
}

func newCoreService(s *coreStore) *core.Service { return core.NewService(s.Store) }

func registerDatabasePoolMetrics(api *apiStore, core *coreStore) error {
	if err := prometheus.Register(observability.NewDatabasePoolCollector("api", api.PoolStats)); err != nil {
		return err
	}
	return prometheus.Register(observability.NewDatabasePoolCollector("core", core.PoolStats))
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

func newAuthManager(c config.Config) (*auth.Manager, error) {
	return auth.NewManager(c.JWTSecret, "go-scheduler", 15*time.Minute)
}

func newHTTPServer(c config.Config, client schedulerv1.SchedulerServiceClient, s *apiStore, manager *auth.Manager) *http.Server {
	handler := apihttp.NewServer(client, s.Store, manager, c.CookieSecure)
	handler.SetContextPath(c.APIContextPath)
	handler.SetStandaloneInstance(c.InstanceID, time.Now())
	return &http.Server{
		Addr:              c.HTTPAddress,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func newEngine(c config.Config, s *coreStore) *core.Engine {
	return core.NewEngine(s.Store, c.InstanceID, c.SchedulerInterval, c.Workers, c.PublicBaseURL, c.HistoryRetention, c.TargetAllowlist)
}

func newNotifier(c config.Config, s *coreStore) *notifier.Worker {
	return notifier.New(s.Store, c.InstanceID, notifier.SMTPConfig{
		Address: c.SMTPAddress, Username: c.SMTPUsername, Password: c.SMTPPassword, From: c.SMTPFrom,
	})
}

func run(lc fx.Lifecycle, server *http.Server, engine *core.Engine, notifications *notifier.Worker) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, stop := context.WithCancel(context.Background())
			cancel = stop
			engine.Run(runCtx)
			notifications.Run(runCtx)
			go func() {
				slog.Info("scheduler server listening", "address", server.Addr, "mode", "standalone")
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("scheduler server stopped", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			err := server.Shutdown(ctx)
			engine.Wait()
			notifications.Wait()
			return err
		},
	})
}
