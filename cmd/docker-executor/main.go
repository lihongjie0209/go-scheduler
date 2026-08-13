//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lihongjie0209/go-scheduler/pkg/executor"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	schedulerURL, token := os.Getenv("SCHEDULER_URL"), os.Getenv("SCHEDULER_TOKEN")
	groupID, nodeID := os.Getenv("EXECUTOR_GROUP_ID"), os.Getenv("EXECUTOR_NODE_ID")
	address, listen := os.Getenv("EXECUTOR_ADVERTISE_URL"), envOr("EXECUTOR_LISTEN", ":9999")
	if schedulerURL == "" || token == "" || groupID == "" || nodeID == "" || address == "" {
		return errors.New("SCHEDULER_URL, SCHEDULER_TOKEN, EXECUTOR_GROUP_ID, EXECUTOR_NODE_ID and EXECUTOR_ADVERTISE_URL are required")
	}
	server, err := executor.NewServer(executor.Options{SchedulerURL: schedulerURL})
	if err != nil {
		return err
	}
	if err = server.Handle("__docker__", executor.DockerHandler(executor.DockerOptions{Binary: envOr("DOCKER_BINARY", "docker")})); err != nil {
		return err
	}
	ttl, err := time.ParseDuration(envOr("EXECUTOR_TTL", "30s"))
	if err != nil {
		return fmt.Errorf("parse EXECUTOR_TTL: %w", err)
	}
	registrar, err := executor.NewRegistrar(executor.RegistrarOptions{APIURL: schedulerURL, Token: token, GroupID: groupID, NodeID: nodeID, Address: address, TTL: ttl})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpServer := &http.Server{Addr: listen, Handler: server, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 0, IdleTimeout: 60 * time.Second}
	httpErr := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpErr <- err
	}()
	registrarErr := make(chan error, 1)
	go func() { registrarErr <- registrar.Run(ctx) }()
	select {
	case <-ctx.Done():
	case err = <-httpErr:
		stop()
	case err = <-registrarErr:
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}
	return shutdownErr
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
