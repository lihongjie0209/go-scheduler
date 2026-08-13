//go:build unix && !linux

package executor

import (
	"os/exec"
	"syscall"
)

func configureScriptProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
