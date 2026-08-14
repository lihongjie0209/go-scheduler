package main

import (
	"os"
	"strings"
	"testing"
)

func TestRootCommandContainsAllRuntimeModes(t *testing.T) {
	want := map[string]bool{"server": false, "api-server": false, "core": false, "executor": false, "migrate": false, "bootstrap": false}
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
