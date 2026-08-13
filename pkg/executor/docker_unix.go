//go:build unix

package executor

import (
	"bytes"
	"context"
	"encoding/base64"
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
		executionID := task.ExternalExecutionID
		if executionID == "" {
			executionID = task.RunID
		}
		name := dockerContainerName(executionID)
		if name == "go-scheduler-" {
			return errors.New("run ID cannot form a container name")
		}
		exists, err := inspectManagedDockerContainer(ctx, binary, name, task)
		if err != nil {
			return err
		}
		cleanup := func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			_ = exec.CommandContext(cleanupCtx, binary, "rm", "--force", name).Run() // #nosec G204 -- fixed Docker arguments.
		}
		if exists {
			defer cleanup()
			return resumeDockerContainer(ctx, binary, name, task.Logger)
		}
		pullImage := func() error {
			environment, cleanupAuth, authErr := dockerRegistryEnvironment(task.DockerRegistryAuth)
			if authErr != nil {
				return authErr
			}
			defer cleanupAuth()
			return runDockerCommandWithEnvironment(ctx, binary, task.Logger, environment, "pull", definition.Image)
		}
		switch definition.PullPolicy {
		case "always":
			if err = pullImage(); err != nil {
				return fmt.Errorf("pull image: %w", err)
			}
		case "if_not_present":
			inspect := exec.CommandContext(ctx, binary, "image", "inspect", definition.Image) // #nosec G204 -- binary is trusted operator configuration and image is passed as one argument.
			if inspect.Run() != nil {
				if err = pullImage(); err != nil {
					return fmt.Errorf("pull image: %w", err)
				}
			}
		}
		defer cleanup()
		arguments := dockerRunArguments(name, task, definition)
		if err = runDockerCommand(ctx, binary, task.Logger, arguments...); err != nil {
			return fmt.Errorf("run container: %w", err)
		}
		return nil
	}
}

func DockerCanceller(options DockerOptions) ExternalCanceller {
	binary := options.Binary
	if binary == "" {
		binary = "docker"
	}
	return func(ctx context.Context, cancellation ExternalCancellation) error {
		if cancellation.RunID == "" || cancellation.ExternalExecutionID == "" || cancellation.JobID == "" {
			return errors.New("run, external execution, and job IDs are required for Docker cancellation")
		}
		if _, err := exec.LookPath(binary); err != nil {
			return fmt.Errorf("find docker client: %w", err)
		}
		name := dockerContainerName(cancellation.ExternalExecutionID)
		if name == "go-scheduler-" {
			return errors.New("external execution ID cannot form a container name")
		}
		task := Task{RunID: cancellation.RunID, ExternalExecutionID: cancellation.ExternalExecutionID, JobID: cancellation.JobID}
		exists, err := inspectManagedDockerContainer(ctx, binary, name, task)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		if err = exec.CommandContext(ctx, binary, "rm", "--force", name).Run(); err != nil { // #nosec G204 -- fixed Docker arguments and internally generated name.
			return fmt.Errorf("remove managed Docker container: %w", err)
		}
		return nil
	}
}

func dockerContainerName(executionID string) string {
	return "go-scheduler-" + strings.Trim(safeContainerName.ReplaceAllString(executionID, "-"), "-.")
}

func inspectManagedDockerContainer(ctx context.Context, binary, name string, task Task) (bool, error) {
	command := exec.CommandContext(ctx, binary, "container", "inspect", name) // #nosec G204 -- binary is trusted operator configuration and name is generated internally.
	var stderr bytes.Buffer
	command.Stderr = &stderr
	raw, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && (strings.Contains(stderr.String(), "No such object") || strings.Contains(stderr.String(), "No such container")) {
			return false, nil
		}
		return false, fmt.Errorf("inspect Docker container: %w", err)
	}
	if err = validateManagedDockerInspection(raw, task); err != nil {
		return false, err
	}
	return true, nil
}

func validateManagedDockerInspection(raw []byte, task Task) error {
	var inspected []struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.Unmarshal(raw, &inspected); err != nil || len(inspected) != 1 {
		return errors.New("decode Docker container inspection")
	}
	labels := inspected[0].Config.Labels
	executionID := task.ExternalExecutionID
	if executionID == "" {
		executionID = task.RunID
	}
	identityMatches := labels["go-scheduler.execution-id"] == executionID || labels["go-scheduler.execution-id"] == "" && labels["go-scheduler.run-id"] == executionID
	if labels["go-scheduler.managed-by"] != "lihongjie0209" || !identityMatches || labels["go-scheduler.job-id"] != task.JobID {
		return errors.New("docker container name is occupied by an unmanaged or different execution")
	}
	return nil
}

func resumeDockerContainer(ctx context.Context, binary, name string, logger TaskLogger) error {
	wait := exec.CommandContext(ctx, binary, "container", "wait", name) // #nosec G204 -- binary is trusted operator configuration and name is generated internally.
	raw, err := wait.Output()
	if err != nil {
		return fmt.Errorf("wait for existing Docker container: %w", err)
	}
	exitCode, err := dockerExitStatus(raw)
	if err != nil {
		return err
	}
	if err = runDockerCommand(ctx, binary, logger, "container", "logs", name); err != nil {
		return fmt.Errorf("read existing Docker container logs: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("existing Docker container exited with status %d", exitCode)
	}
	return nil
}

func dockerExitStatus(raw []byte) (int, error) {
	exitCode, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || exitCode < 0 || exitCode > 255 {
		return 0, errors.New("decode existing Docker container exit code")
	}
	return exitCode, nil
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
		return DockerDefinition{}, errors.New("docker image must be a non-empty reference of at most 512 bytes")
	}
	if definition.PullPolicy == "" {
		definition.PullPolicy = "if_not_present"
	}
	if definition.PullPolicy != "always" && definition.PullPolicy != "if_not_present" && definition.PullPolicy != "never" {
		return DockerDefinition{}, errors.New("pull_policy must be always, if_not_present, or never")
	}
	if len(definition.Network) > 256 || strings.ContainsAny(definition.Network, " \t\r\n") {
		return DockerDefinition{}, errors.New("network must be a Docker network name without whitespace")
	}
	if definition.MemoryMB < 0 || definition.MemoryMB > 1048576 || definition.CPUs < 0 || definition.CPUs > 1024 {
		return DockerDefinition{}, errors.New("invalid Docker resource limit")
	}
	if len(definition.Command)+len(definition.Args) > 256 || len(definition.Env) > 128 {
		return DockerDefinition{}, errors.New("docker command, arguments, or environment exceeds limits")
	}
	return definition, nil
}

func dockerRunArguments(name string, task Task, definition DockerDefinition) []string {
	executionID := task.ExternalExecutionID
	if executionID == "" {
		executionID = task.RunID
	}
	arguments := []string{"run", "--name", name, "--label", "go-scheduler.managed-by=lihongjie0209", "--label", "go-scheduler.execution-id=" + executionID, "--label", "go-scheduler.run-id=" + task.RunID, "--label", "go-scheduler.job-id=" + task.JobID}
	if definition.Network != "" {
		arguments = append(arguments, "--network", definition.Network)
	}
	if definition.ReadOnlyRoot != nil && *definition.ReadOnlyRoot {
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
	return runDockerCommandWithEnvironment(ctx, binary, logger, os.Environ(), arguments...)
}

func runDockerCommandWithEnvironment(ctx context.Context, binary string, logger TaskLogger, environment []string, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...) // #nosec G204 -- all values are passed as arguments without shell evaluation.
	command.Env = environment
	stdout, stderr := &limitedBuffer{limit: maxScriptOutputBytes}, &limitedBuffer{limit: maxScriptOutputBytes}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	if logErr := writeScriptOutput(logger, stdout.String(), stderr.String()); logErr != nil && err == nil {
		err = logErr
	}
	if stdout.exceeded || stderr.exceeded {
		return errors.New("docker output exceeded 1 MiB")
	}
	return err
}

func dockerRegistryEnvironment(auth *DockerRegistryAuth) ([]string, func(), error) {
	if auth == nil {
		return os.Environ(), func() {}, nil
	}
	if strings.TrimSpace(auth.Server) == "" || strings.TrimSpace(auth.Username) == "" || auth.Password == "" {
		return nil, nil, errors.New("docker registry server, username and password are required")
	}
	if strings.ContainsAny(auth.Server, " \t\r\n") || strings.ContainsAny(auth.Username, ":\r\n") {
		return nil, nil, errors.New("invalid docker registry server or username")
	}
	directory, err := os.MkdirTemp("", "go-scheduler-docker-auth-")
	if err != nil {
		return nil, nil, fmt.Errorf("create temporary Docker credentials: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	config := struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}{Auths: map[string]struct {
		Auth string `json:"auth"`
	}{strings.TrimSpace(auth.Server): {Auth: base64.StdEncoding.EncodeToString([]byte(strings.TrimSpace(auth.Username) + ":" + auth.Password))}}}
	raw, err := json.Marshal(config)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("encode temporary Docker credentials: %w", err)
	}
	if err = os.WriteFile(directory+string(os.PathSeparator)+"config.json", raw, 0o600); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("write temporary Docker credentials: %w", err)
	}
	environment := os.Environ()
	filtered := make([]string, 0, len(environment)+1)
	for _, value := range environment {
		if !strings.HasPrefix(value, "DOCKER_CONFIG=") {
			filtered = append(filtered, value)
		}
	}
	return append(filtered, "DOCKER_CONFIG="+directory), cleanup, nil
}
