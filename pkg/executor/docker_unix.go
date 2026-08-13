//go:build unix

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var safeContainerName = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type DockerOptions struct {
	Binary string
}

type DockerDefinition struct {
	Image        string            `json:"image"`
	Command      []string          `json:"command,omitempty"`
	Args         []string          `json:"args,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	PullPolicy   string            `json:"pull_policy,omitempty"`
	Network      string            `json:"network,omitempty"`
	ReadOnlyRoot *bool             `json:"read_only_root,omitempty"`
	MemoryMB     int64             `json:"memory_mb,omitempty"`
	CPUs         float64           `json:"cpus,omitempty"`
}

func DockerHandler(options DockerOptions) Handler {
	binary := options.Binary
	if binary == "" {
		binary = "docker"
	}
	return func(ctx context.Context, task Task) error {
		definition, err := parseDockerDefinition(task.ScriptSource)
		if err != nil {
			return err
		}
		if _, err = exec.LookPath(binary); err != nil {
			return fmt.Errorf("find docker client: %w", err)
		}
		if definition.PullPolicy == "always" {
			if err = runDockerCommand(ctx, binary, task.Logger, "pull", definition.Image); err != nil {
				return fmt.Errorf("pull image: %w", err)
			}
		} else if definition.PullPolicy == "if_not_present" {
			inspect := exec.CommandContext(ctx, binary, "image", "inspect", definition.Image) // #nosec G204 -- binary is trusted operator configuration and image is passed as one argument.
			if inspect.Run() != nil {
				if err = runDockerCommand(ctx, binary, task.Logger, "pull", definition.Image); err != nil {
					return fmt.Errorf("pull image: %w", err)
				}
			}
		}
		name := "go-scheduler-" + strings.Trim(safeContainerName.ReplaceAllString(task.RunID, "-"), "-.")
		if name == "go-scheduler-" {
			return errors.New("run ID cannot form a container name")
		}
		cleanup := func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			_ = exec.CommandContext(cleanupCtx, binary, "rm", "--force", name).Run() // #nosec G204 -- fixed Docker arguments.
		}
		defer cleanup()
		arguments := dockerRunArguments(name, task, definition)
		if err = runDockerCommand(ctx, binary, task.Logger, arguments...); err != nil {
			return fmt.Errorf("run container: %w", err)
		}
		return nil
	}
}

func parseDockerDefinition(source string) (DockerDefinition, error) {
	var definition DockerDefinition
	decoder := json.NewDecoder(strings.NewReader(source))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&definition); err != nil {
		return DockerDefinition{}, fmt.Errorf("decode Docker definition: %w", err)
	}
	definition.Image = strings.TrimSpace(definition.Image)
	if definition.Image == "" || len(definition.Image) > 512 || strings.ContainsAny(definition.Image, " \t\r\n") {
		return DockerDefinition{}, errors.New("Docker image must be a non-empty reference of at most 512 bytes")
	}
	if definition.PullPolicy == "" {
		definition.PullPolicy = "if_not_present"
	}
	if definition.PullPolicy != "always" && definition.PullPolicy != "if_not_present" && definition.PullPolicy != "never" {
		return DockerDefinition{}, errors.New("pull_policy must be always, if_not_present, or never")
	}
	if definition.Network == "" {
		definition.Network = "none"
	}
	if definition.Network != "none" && definition.Network != "bridge" {
		return DockerDefinition{}, errors.New("network must be none or bridge")
	}
	if definition.MemoryMB < 0 || definition.MemoryMB > 1048576 || definition.CPUs < 0 || definition.CPUs > 1024 {
		return DockerDefinition{}, errors.New("invalid Docker resource limit")
	}
	if len(definition.Command)+len(definition.Args) > 256 || len(definition.Env) > 128 {
		return DockerDefinition{}, errors.New("Docker command, arguments, or environment exceeds limits")
	}
	return definition, nil
}

func dockerRunArguments(name string, task Task, definition DockerDefinition) []string {
	arguments := []string{"run", "--rm", "--name", name, "--network", definition.Network, "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "256"}
	readOnly := true
	if definition.ReadOnlyRoot != nil {
		readOnly = *definition.ReadOnlyRoot
	}
	if readOnly {
		arguments = append(arguments, "--read-only")
	}
	if definition.MemoryMB > 0 {
		arguments = append(arguments, "--memory", strconv.FormatInt(definition.MemoryMB, 10)+"m")
	}
	if definition.CPUs > 0 {
		arguments = append(arguments, "--cpus", strconv.FormatFloat(definition.CPUs, 'f', -1, 64))
	}
	environment := make(map[string]string, len(definition.Env)+5)
	for key, value := range definition.Env {
		environment[key] = value
	}
	environment["SCHEDULER_INPUT"] = task.Input
	environment["SCHEDULER_RUN_ID"] = task.RunID
	environment["SCHEDULER_JOB_ID"] = task.JobID
	environment["SCHEDULER_SHARD_INDEX"] = strconv.FormatInt(int64(task.BroadcastIndex), 10)
	environment["SCHEDULER_SHARD_TOTAL"] = strconv.FormatInt(int64(task.BroadcastTotal), 10)
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		arguments = append(arguments, "--env", key+"="+environment[key])
	}
	arguments = append(arguments, definition.Image)
	arguments = append(arguments, definition.Command...)
	arguments = append(arguments, definition.Args...)
	return arguments
}

func runDockerCommand(ctx context.Context, binary string, logger TaskLogger, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...) // #nosec G204 -- all values are passed as arguments without shell evaluation.
	command.Env = os.Environ()
	stdout, stderr := &limitedBuffer{limit: maxScriptOutputBytes}, &limitedBuffer{limit: maxScriptOutputBytes}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	if logErr := writeScriptOutput(logger, stdout.String(), stderr.String()); logErr != nil && err == nil {
		err = logErr
	}
	if stdout.exceeded || stderr.exceeded {
		return errors.New("Docker output exceeded 1 MiB")
	}
	return err
}
