// Package runtime contains infrastructure constructors shared by application modes.
package runtime

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/config"
	"github.com/lihongjie0209/go-scheduler/internal/discovery"
	"github.com/lihongjie0209/go-scheduler/internal/store"
)

// LoggingOption routes Fx lifecycle and dependency graph events through the
// same structured logger used by the application.
func LoggingOption() fx.Option {
	return fx.WithLogger(func() fxevent.Logger {
		logger := &fxevent.SlogLogger{Logger: slog.Default()}
		logger.UseLogLevel(slog.LevelDebug)
		return logger
	})
}

func OpenStore(lc fx.Lifecycle, databaseURL string, cipher store.HeaderCipher, maxConns, minConns int32) (*store.Store, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.New(ctx, databaseURL, store.WithHeaderCipher(cipher), store.WithPoolSize(maxConns, minConns))
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		s.Close()
		return nil
	}})
	return s, nil
}

func NewEtcd(lc fx.Lifecycle, c config.Config) (*clientv3.Client, error) {
	if c.DiscoveryMode == "kubernetes" {
		return nil, nil
	}
	client, err := discovery.NewClient(c.EtcdEndpoints, c.EtcdUsername, c.EtcdPassword, c.EtcdCA, c.EtcdCert, c.EtcdKey)
	if err == nil {
		lc.Append(fx.Hook{OnStop: func(context.Context) error { return client.Close() }})
	}
	return client, err
}

func NewAuthManager(c config.Config) (*auth.Manager, error) {
	return auth.NewManager(c.JWTSecret, "go-scheduler", 15*time.Minute)
}

func NewHTTPServer(address string, handler http.Handler, readTimeout, writeTimeout time.Duration) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, IdleTimeout: 60 * time.Second}
}
