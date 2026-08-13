package executor

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestScriptHandlerExecutesShellWithInputAndLogs(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh unavailable")
	}
	logger := &recordingLogger{}
	handler := ScriptHandler(ScriptOptions{Languages: []string{"shell"}})
	err := handler(t.Context(), Task{Input: "hello world", ScriptLanguage: "shell", ScriptSource: `printf 'out:%s' "$SCHEDULER_INPUT"; printf 'err' >&2`, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(logger.stdout, "") != "out:hello world" || strings.Join(logger.stderr, "") != "err" {
		t.Fatalf("stdout=%v stderr=%v", logger.stdout, logger.stderr)
	}
}

func TestScriptHandlerExecutesNodeJSAndPHP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, language, interpreter, source, wantOut, wantErr string
	}{
		{name: "nodejs", language: "nodejs", interpreter: "node", source: `process.stdout.write("node:" + process.env.SCHEDULER_INPUT); process.stderr.write("node-err")`, wantOut: "node:payload", wantErr: "node-err"},
		{name: "php", language: "php", interpreter: "php", source: `<?php fwrite(STDOUT, "php:" . getenv("SCHEDULER_INPUT")); fwrite(STDERR, "php-err");`, wantOut: "php:payload", wantErr: "php-err"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := exec.LookPath(test.interpreter); err != nil {
				t.Skipf("%s unavailable", test.interpreter)
			}
			logger := &recordingLogger{}
			err := ScriptHandler(ScriptOptions{Languages: []string{test.language}})(t.Context(), Task{Input: "payload", ScriptLanguage: test.language, ScriptSource: test.source, Logger: logger})
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(logger.stdout, ""); got != test.wantOut {
				t.Fatalf("stdout=%q", got)
			}
			if got := strings.Join(logger.stderr, ""); got != test.wantErr {
				t.Fatalf("stderr=%q", got)
			}
		})
	}
}

func TestScriptHandlerExecutesPowerShell(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("pwsh"); err != nil {
		t.Skip("pwsh unavailable; the image-level integration test executes it")
	}
	logger := &recordingLogger{}
	err := ScriptHandler(ScriptOptions{Languages: []string{"powershell"}})(t.Context(), Task{Input: "payload", ScriptLanguage: "powershell", ScriptSource: `[Console]::Out.Write("pwsh:" + $env:SCHEDULER_INPUT); [Console]::Error.Write("pwsh-err")`, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(logger.stdout, "") != "pwsh:payload" || strings.Join(logger.stderr, "") != "pwsh-err" {
		t.Fatalf("stdout=%v stderr=%v", logger.stdout, logger.stderr)
	}
}

func TestScriptHandlerRejectsLanguageAndPropagatesExit(t *testing.T) {
	t.Parallel()
	handler := ScriptHandler(ScriptOptions{Languages: []string{"shell"}})
	if err := handler(t.Context(), Task{ScriptLanguage: "python", ScriptSource: "print(1)", Logger: &recordingLogger{}}); err == nil {
		t.Fatal("unsupported language accepted")
	}
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh unavailable")
	}
	err := handler(t.Context(), Task{ScriptLanguage: "shell", ScriptSource: "exit 7", Logger: &recordingLogger{}})
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("error=%v", err)
	}
}

func TestScriptHandlerHonorsCancellation(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := ScriptHandler(ScriptOptions{Languages: []string{"shell"}})(ctx, Task{ScriptLanguage: "shell", ScriptSource: "sleep 10", Logger: &recordingLogger{}})
	if err == nil || ctx.Err() == nil {
		t.Fatalf("error=%v context=%v", err, ctx.Err())
	}
}

type recordingLogger struct{ stdout, stderr []string }

func (l *recordingLogger) Info(content string) error {
	l.stdout = append(l.stdout, content)
	return nil
}
func (l *recordingLogger) Error(content string) error {
	l.stderr = append(l.stderr, content)
	return nil
}
