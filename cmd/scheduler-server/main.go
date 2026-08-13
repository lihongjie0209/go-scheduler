package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	apihttp "github.com/lihongjie0209/go-scheduler/internal/api"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/core"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/notifier"
	"github.com/lihongjie0209/go-scheduler/internal/rpc"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	fx.New(
		fx.Provide(
			loadConfig,
			newStore,
			core.NewService,
			newInProcessScheduler,
			newCoreClient,
			newAuthManager,
			newHTTPServer,
			newEngine,
			newNotifier,
		),
		fx.Invoke(run),
	).Run()
}

func loadConfig() (config.Config, error) { return config.Load("scheduler-server") }

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
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		s.Close()
		return nil
	}})
	return s, nil
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

func newHTTPServer(c config.Config, client schedulerv1.SchedulerServiceClient, s *store.Store, manager *auth.Manager) *http.Server {
	handler := apihttp.NewServer(client, s, manager, c.CookieSecure)
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

func newEngine(c config.Config, s *store.Store) *core.Engine {
	return core.NewEngine(s, c.InstanceID, c.SchedulerInterval, c.Workers, c.PublicBaseURL, c.HistoryRetention, c.TargetAllowlist)
}

func newNotifier(c config.Config, s *store.Store) *notifier.Worker {
	return notifier.New(s, c.InstanceID, notifier.SMTPConfig{
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
