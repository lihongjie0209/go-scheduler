package main

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestRootCommandContainsAllRuntimeModes(t *testing.T) {
	want := map[string]bool{"standalone": false, "api-server": false, "scheduler-core": false, "executor": false, "migrate": false, "bootstrap": false}
	for _, command := range newRootCommand().Commands() {
		if _, ok := want[command.Name()]; ok {
			want[command.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing %q subcommand", name)
		}
	}
}

func TestLegacyServerCommandIsRejected(t *testing.T) {
	for _, legacyName := range []string{"server", "core"} {
		command := newRootCommand()
		command.SetArgs([]string{legacyName})
		if err := command.Execute(); err == nil {
			t.Errorf("legacy %s command succeeded", legacyName)
		}
	}
}

func TestRootCommandLoggingFlags(t *testing.T) {
	command := newRootCommand()
	for _, name := range []string{"log-level", "log-format", "log-source"} {
		if command.PersistentFlags().Lookup(name) == nil {
			t.Errorf("missing --%s persistent flag", name)
		}
	}
}

func TestRuntimeCommandsInvokeExpectedRunner(t *testing.T) {
	for _, test := range []struct {
		name    string
		command string
	}{
		{name: "standalone", command: "standalone"},
		{name: "API server", command: "api-server"},
		{name: "scheduler core", command: "scheduler-core"},
		{name: "executor", command: "executor"},
		{name: "migrate", command: "migrate"},
		{name: "bootstrap", command: "bootstrap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := ""
			command := newTestRootCommand(&called, &bytes.Buffer{})
			command.SetArgs([]string{test.command})
			if err := command.Execute(); err != nil {
				t.Fatal(err)
			}
			if called != test.command {
				t.Fatalf("runner = %q, want %q", called, test.command)
			}
		})
	}
}

func TestRuntimeCommandValidationDoesNotInvokeRunner(t *testing.T) {
	called := ""
	command := newTestRootCommand(&called, &bytes.Buffer{})
	command.SetArgs([]string{"standalone", "unexpected"})
	if err := command.Execute(); err == nil {
		t.Fatal("command with positional argument succeeded")
	}
	if called != "" {
		t.Fatalf("runner was called: %q", called)
	}
}

func TestLoggingFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("LOG_FORMAT", "json")
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	var output bytes.Buffer
	called := ""
	command := newTestRootCommand(&called, &output)
	command.SetArgs([]string{"standalone", "--log-level=debug", "--log-format=text"})
	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if called != "standalone" {
		t.Fatalf("runner = %q, want standalone", called)
	}
	if logOutput := output.String(); !strings.Contains(logOutput, "level=INFO") || !strings.Contains(logOutput, "service=scheduler-standalone") {
		t.Fatalf("unexpected log output: %q", logOutput)
	}
}

func TestInvalidLoggingConfigurationDoesNotInvokeRunner(t *testing.T) {
	called := ""
	command := newTestRootCommand(&called, &bytes.Buffer{})
	command.SetArgs([]string{"scheduler-core", "--log-format=xml"})
	if err := command.Execute(); err == nil {
		t.Fatal("command with invalid log format succeeded")
	}
	if called != "" {
		t.Fatalf("runner was called: %q", called)
	}
}

func TestRunnerErrorIsReturned(t *testing.T) {
	want := errors.New("start failed")
	dependencies := testDependencies(nil, &bytes.Buffer{})
	dependencies.core = func() error { return want }
	command := newRootCommandWithDependencies(dependencies)
	command.SetArgs([]string{"scheduler-core"})
	if err := command.Execute(); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func newTestRootCommand(called *string, output *bytes.Buffer) interface {
	SetArgs([]string)
	Execute() error
} {
	return newRootCommandWithDependencies(testDependencies(called, output))
}

func testDependencies(called *string, output *bytes.Buffer) commandDependencies {
	runner := func(name string) func() error {
		return func() error {
			if called != nil {
				*called = name
			}
			return nil
		}
	}
	return commandDependencies{
		output: output, standalone: runner("standalone"), apiServer: runner("api-server"),
		core: runner("scheduler-core"), executor: runner("executor"), migrate: runner("migrate"), bootstrap: runner("bootstrap"),
	}
}

func TestDockerReleaseInjectsVersion(t *testing.T) {
	t.Parallel()
	dockerfile, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"ARG VERSION=dev", "-X main.version=${VERSION}"} {
		if !strings.Contains(string(dockerfile), required) {
			t.Errorf("Dockerfile does not contain %q", required)
		}
	}
	workflow, err := os.ReadFile("../../.github/workflows/docker.yml")
	if err != nil {
		t.Fatal(err)
	}
	if required := "VERSION=${{ github.ref_type == 'tag' && github.ref_name || github.sha }}"; !strings.Contains(string(workflow), required) {
		t.Errorf("Docker workflow does not contain %q", required)
	}
}
