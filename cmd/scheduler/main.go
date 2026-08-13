package main

import (
	"fmt"
	"os"

	apiserver "github.com/lihongjie0209/go-scheduler/cmd/api-server"
	bootstrapcmd "github.com/lihongjie0209/go-scheduler/cmd/bootstrap"
	migratecmd "github.com/lihongjie0209/go-scheduler/cmd/migrate"
	schedulercore "github.com/lihongjie0209/go-scheduler/cmd/scheduler-core"
	schedulerserver "github.com/lihongjie0209/go-scheduler/cmd/scheduler-server"
	scriptexecutor "github.com/lihongjie0209/go-scheduler/cmd/script-executor"
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
	root := &cobra.Command{
		Use:           "scheduler",
		Short:         "Go Scheduler control-plane and executor runtime",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	root.AddCommand(
		runCommand("server", "Run API Server and Core in one process", schedulerserver.Run),
		runCommand("api-server", "Run the distributed API Server", apiserver.Run),
		runCommand("core", "Run the distributed scheduler Core", schedulercore.Run),
		&cobra.Command{Use: "executor", Short: "Run the script, HTTP, Docker, and Kubernetes executor", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error {
			return scriptexecutor.Run()
		}},
		runCommand("migrate", "Apply PostgreSQL migrations", migratecmd.Run),
		runCommand("bootstrap", "Create the initial tenant and administrator", bootstrapcmd.Run),
	)
	return root
}

func runCommand(use, short string, run func()) *cobra.Command {
	return &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, Run: func(*cobra.Command, []string) { run() }}
}
