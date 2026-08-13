package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	schedulerURL := os.Getenv("SCHEDULER_URL")
	token, groupID := os.Getenv("SCHEDULER_TOKEN"), os.Getenv("EXECUTOR_GROUP_ID")
	nodeID, address := os.Getenv("EXECUTOR_NODE_ID"), os.Getenv("EXECUTOR_ADVERTISE_URL")
	listen := envOr("EXECUTOR_LISTEN", ":9999")
	if schedulerURL == "" || token == "" || groupID == "" || nodeID == "" || address == "" {
		return errors.New("SCHEDULER_URL, SCHEDULER_TOKEN, EXECUTOR_GROUP_ID, EXECUTOR_NODE_ID and EXECUTOR_ADVERTISE_URL are required")
	}
	languages := splitLanguages(envOr("SCRIPT_LANGUAGES", "shell,python,nodejs,php,powershell"))
	server, err := executor.NewServer(executor.Options{SchedulerURL: schedulerURL})
	if err != nil {
		return err
	}
	if err = server.Handle("__script__", executor.ScriptHandler(executor.ScriptOptions{Languages: languages})); err != nil {
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
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
			return
		}
		httpErr <- nil
	}()
	registrarErr := make(chan error, 1)
	go func() { registrarErr <- registrar.Run(ctx) }()
	select {
	case <-ctx.Done():
	case err = <-httpErr:
		if err != nil {
			stop()
		}
	case err = <-registrarErr:
		if err != nil {
			stop()
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	if err != nil {
		return err
	}
	return shutdownErr
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
