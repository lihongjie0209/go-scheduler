package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/perfbench"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("scheduler benchmark failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return serve([]string{})
	}
	if strings.HasPrefix(args[0], "-") {
		return serve(args)
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "load":
		return load(args[1:])
	default:
		return fmt.Errorf("unknown command %q; expected serve or load", args[0])
	}
}

func serve(args []string) error {
	flags := flag.NewFlagSet("scheduler-bench serve", flag.ContinueOnError)
	listen := flags.String("listen", ":19090", "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("serve does not accept positional arguments")
	}
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
		return err
	}
	return nil
}

func load(args []string) error {
	flags := flag.NewFlagSet("scheduler-bench load", flag.ContinueOnError)
	system := flags.String("system", "", "scheduler to load: go or xxl")
	serverURL := flags.String("server", "", "scheduler API base URL")
	sinkURL := flags.String("sink", "", "benchmark sink execution URL")
	runID := flags.String("run-id", "", "unique benchmark run ID")
	count := flags.Int("count", 100, "number of scheduled jobs")
	concurrency := flags.Int("concurrency", 8, "parallel setup requests")
	scheduledAtText := flags.String("scheduled-at", "", "shared future schedule time in RFC3339")
	timeout := flags.Duration("timeout", 30*time.Minute, "maximum setup duration")
	tenantID := flags.String("tenant", os.Getenv("BENCH_TENANT_ID"), "Go Scheduler tenant ID")
	xxlGroup := flags.Int("xxl-executor-group", envInt("BENCH_XXL_EXECUTOR_GROUP", 0), "XXL-JOB executor group ID")
	xxlHandler := flags.String("xxl-handler", "schedulerBenchmarkHandler", "XXL-JOB benchmark executor handler")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *serverURL == "" || *sinkURL == "" || *runID == "" || *scheduledAtText == "" {
		return fmt.Errorf("system, server, sink, run-id, and scheduled-at are required")
	}
	scheduledAt, err := time.Parse(time.RFC3339, *scheduledAtText)
	if err != nil {
		return fmt.Errorf("parse scheduled-at: %w", err)
	}
	if time.Until(scheduledAt) < 30*time.Second {
		return fmt.Errorf("scheduled-at must be at least 30 seconds in the future")
	}

	var loader perfbench.JobLoader
	switch *system {
	case "go":
		token := os.Getenv("BENCH_TOKEN")
		if token == "" {
			return fmt.Errorf("benchmark token environment variable BENCH_TOKEN is required for Go Scheduler")
		}
		loader = &perfbench.GoSchedulerLoader{BaseURL: *serverURL, Token: token, TenantID: *tenantID}
	case "xxl":
		xxl := &perfbench.XXLJobLoader{BaseURL: *serverURL, Username: os.Getenv("BENCH_XXL_USERNAME"), Password: os.Getenv("BENCH_XXL_PASSWORD"), ExecutorGroupID: *xxlGroup, ExecutorHandler: *xxlHandler}
		loginCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err = xxl.Login(loginCtx)
		cancel()
		if err != nil {
			return err
		}
		loader = xxl
	default:
		return fmt.Errorf("system must be go or xxl")
	}

	events, err := perfbench.BuildExpectedEvents(*runID, *count, scheduledAt)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, *timeout)
	defer timeoutCancel()
	if err = perfbench.RegisterExpectations(ctx, nil, *sinkURL, events); err != nil {
		return fmt.Errorf("register sink expectations: %w", err)
	}
	loaded, err := perfbench.LoadScheduledJobs(ctx, loader, perfbench.LoadRequest{RunID: *runID, Count: *count, Concurrency: *concurrency, ScheduledAt: scheduledAt, SinkURL: *sinkURL})
	if err != nil {
		return err
	}
	if !time.Now().Before(scheduledAt) {
		return fmt.Errorf("job setup completed after scheduled-at; discard this run and choose a later time")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"system": *system, "run_id": *runID, "scheduled_at": scheduledAt.UTC(), "jobs": loaded})
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}
