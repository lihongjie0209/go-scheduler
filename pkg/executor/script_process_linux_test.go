//go:build linux

package executor

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureScriptProcessKillsProcessGroupWithParent(t *testing.T) {
	t.Parallel()
	command := exec.Command("true")
	configureScriptProcess(command)
	if command.SysProcAttr == nil || !command.SysProcAttr.Setpgid || command.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("script process attributes = %+v", command.SysProcAttr)
	}
}
