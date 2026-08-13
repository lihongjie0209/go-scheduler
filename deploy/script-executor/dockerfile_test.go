package scriptexecutor_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDockerfileRejectsArchitecturesWithoutPowerShellMuslBuild(t *testing.T) {
	dockerfile, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	content := string(dockerfile)
	if !strings.Contains(content, `test "${TARGETARCH}" = "amd64"`) {
		t.Fatal("executor Dockerfile does not reject unsupported PowerShell architectures")
	}
	if !strings.Contains(content, "ARG TARGETARCH=amd64") {
		t.Fatal("executor Dockerfile does not default legacy builder architecture")
	}
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "docker.yml"))
	if err != nil {
		t.Fatal(err)
	}
	marker := "file: deploy/script-executor/Dockerfile"
	index := strings.Index(string(workflow), marker)
	if index < 0 {
		t.Fatal("executor image build is missing from Docker workflow")
	}
	build := string(workflow)[index:]
	if !strings.Contains(build, "platforms: linux/amd64") || strings.Contains(strings.SplitN(build, "platforms:", 2)[1], "linux/arm64") {
		t.Fatal("executor workflow publishes an architecture without a PowerShell musl build")
	}
}
