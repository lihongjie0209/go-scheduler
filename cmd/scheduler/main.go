package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"

	bootstrapcmd "github.com/lihongjie0209/go-scheduler/cmd/bootstrap"
	migratecmd "github.com/lihongjie0209/go-scheduler/cmd/migrate"
	scriptexecutor "github.com/lihongjie0209/go-scheduler/cmd/script-executor"
	"github.com/lihongjie0209/go-scheduler/internal/app/apiserver"
	"github.com/lihongjie0209/go-scheduler/internal/app/schedulercore"
	"github.com/lihongjie0209/go-scheduler/internal/app/schedulerserver"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	if err := newRootCommand().Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	return newRootCommandWithDependencies(commandDependencies{
		output:     os.Stderr,
		standalone: func() error { schedulerserver.Run(); return nil },
		apiServer:  func() error { apiserver.Run(); return nil },
		core:       func() error { schedulercore.Run(); return nil },
		executor:   scriptexecutor.Run,
		migrate:    func() error { migratecmd.Run(); return nil },
		bootstrap:  func() error { bootstrapcmd.Run(); return nil },
	})
}

type commandDependencies struct {
	output                                io.Writer
	standalone, apiServer, core, executor func() error
	migrate, bootstrap                    func() error
}

func newRootCommandWithDependencies(dependencies commandDependencies) *cobra.Command {
	logging := observability.LoggingConfig{
		Level:     environment("LOG_LEVEL", "info"),
		Format:    environment("LOG_FORMAT", "json"),
		AddSource: environmentBool("LOG_SOURCE", false),
		Version:   version,
	}
	root := &cobra.Command{
		Use:           "scheduler",
		Short:         "Go Scheduler control-plane and executor runtime",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	root.PersistentFlags().StringVar(&logging.Level, "log-level", logging.Level, "log level: debug, info, warn, or error")
	root.PersistentFlags().StringVar(&logging.Format, "log-format", logging.Format, "log format: json or text")
	root.PersistentFlags().BoolVar(&logging.AddSource, "log-source", logging.AddSource, "include source file and line in logs")
	root.AddCommand(
		runCommand("standalone", "Run API Server and Core in one process", "scheduler-standalone", &logging, dependencies.output, dependencies.standalone),
		runCommand("api-server", "Run the distributed API Server", "api-server", &logging, dependencies.output, dependencies.apiServer),
		runCommand("scheduler-core", "Run the distributed scheduler service", "scheduler-core", &logging, dependencies.output, dependencies.core),
		&cobra.Command{Use: "executor", Short: "Run the script, HTTP, Docker, and Kubernetes executor", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
			if err := configureLogger(dependencies.output, logging, "executor"); err != nil {
				return err
			}
			return dependencies.executor()
		}},
		runCommand("migrate", "Apply PostgreSQL migrations", "migrate", &logging, dependencies.output, dependencies.migrate),
		runCommand("bootstrap", "Create the initial tenant and administrator", "bootstrap", &logging, dependencies.output, dependencies.bootstrap),
	)
	return root
}

func runCommand(use, short, service string, logging *observability.LoggingConfig, output io.Writer, run func() error) *cobra.Command {
	return &cobra.Command{
		Use: use, Short: short, Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if err := configureLogger(output, *logging, service); err != nil {
				return err
			}
			slog.Info("service starting")
			if err := run(); err != nil {
				return err
			}
			slog.Info("service stopped")
			return nil
		},
	}
}

func configureLogger(output io.Writer, config observability.LoggingConfig, service string) error {
	config.Service = service
	logger, err := observability.NewLogger(output, config)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

func environment(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func environmentBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
