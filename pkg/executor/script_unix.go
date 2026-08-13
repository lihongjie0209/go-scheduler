//go:build unix

package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

const maxScriptOutputBytes = 1 << 20

type ScriptOptions struct{ Languages []string }

func ScriptHandler(options ScriptOptions) Handler {
	allowed := make(map[string]bool, len(options.Languages))
	for _, language := range options.Languages {
		allowed[language] = true
	}
	return func(ctx context.Context, task Task) error {
		if !allowed[task.ScriptLanguage] {
			return fmt.Errorf("script language %q is disabled", task.ScriptLanguage)
		}
		if task.ScriptSource == "" || len(task.ScriptSource) > 1<<20 {
			return errors.New("script source must be between 1 byte and 1 MiB")
		}
		interpreter := map[string]string{"shell": "sh", "python": "python3", "nodejs": "node", "php": "php", "powershell": "pwsh"}[task.ScriptLanguage]
		path, err := exec.LookPath(interpreter)
		if err != nil {
			return fmt.Errorf("find %s interpreter: %w", task.ScriptLanguage, err)
		}
		script, err := os.CreateTemp("", "go-scheduler-script-*")
		if err != nil {
			return err
		}
		scriptName := script.Name()
		defer func() { _ = os.Remove(scriptName) }()
		if err = script.Chmod(0o700); err == nil {
			_, err = io.WriteString(script, task.ScriptSource)
		}
		closeErr := script.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		command := exec.Command(path, scriptName) // #nosec G204 -- executable is selected from a fixed language map; source is passed as a file, never shell-concatenated.
		command.Env = append(os.Environ(), "SCHEDULER_INPUT="+task.Input, "SCHEDULER_RUN_ID="+task.RunID, "SCHEDULER_JOB_ID="+task.JobID, fmt.Sprintf("SCHEDULER_SHARD_INDEX=%d", task.BroadcastIndex), fmt.Sprintf("SCHEDULER_SHARD_TOTAL=%d", task.BroadcastTotal))
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdout, stderr := &limitedBuffer{limit: maxScriptOutputBytes}, &limitedBuffer{limit: maxScriptOutputBytes}
		command.Stdout, command.Stderr = stdout, stderr
		if err = command.Start(); err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		select {
		case err = <-done:
		case <-ctx.Done():
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			<-done
			err = ctx.Err()
		}
		if logErr := writeScriptOutput(task.Logger, stdout.String(), stderr.String()); logErr != nil && err == nil {
			err = logErr
		}
		if stdout.exceeded || stderr.exceeded {
			return errors.New("script output exceeded 1 MiB")
		}
		return err
	}
}

type limitedBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	remaining := w.limit - w.buffer.Len()
	if remaining <= 0 {
		w.exceeded = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = w.buffer.Write(p[:remaining])
		w.exceeded = true
		return len(p), nil
	}
	_, _ = w.buffer.Write(p)
	return len(p), nil
}
func (w *limitedBuffer) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.buffer.String() }
func writeScriptOutput(logger TaskLogger, stdout, stderr string) error {
	if logger == nil {
		return nil
	}
	for _, item := range []struct {
		value string
		write func(string) error
	}{{stdout, logger.Info}, {stderr, logger.Error}} {
		for len(item.value) > 0 {
			size := min(len(item.value), 65536)
			if err := item.write(item.value[:size]); err != nil {
				return err
			}
			item.value = item.value[size:]
		}
	}
	return nil
}

var _ io.Writer = (*limitedBuffer)(nil)
