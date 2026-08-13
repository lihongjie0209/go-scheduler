package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/perfbench"
)

func main() {
	listen := flag.String("listen", ":19090", "HTTP listen address")
	flag.Parse()

	server := &http.Server{
		Addr:              *listen,
		Handler:           perfbench.NewServer(perfbench.NewCollector()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("benchmark sink shutdown failed", "error", err)
		}
	}()
	slog.Info("benchmark sink listening", "address", *listen)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("benchmark sink failed", "error", err)
		os.Exit(1)
	}
}
