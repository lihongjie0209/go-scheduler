package main

import "testing"

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
