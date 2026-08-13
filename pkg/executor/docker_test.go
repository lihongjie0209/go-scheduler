//go:build unix

package executor

import (
	"reflect"
	"testing"
)

func TestParseDockerDefinition(t *testing.T) {
	tests := []struct {
		name, source string
		wantErr      bool
	}{
		{name: "minimal", source: `{"image":"alpine:3.22"}`},
		{name: "private image", source: `{"image":"registry.example.com/team/job:v1","pull_policy":"always","network":"bridge","memory_mb":256,"cpus":0.5}`},
		{name: "missing image", source: `{}`, wantErr: true},
		{name: "unknown field", source: `{"image":"alpine","privileged":true}`, wantErr: true},
		{name: "invalid network", source: `{"image":"alpine","network":"host"}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseDockerDefinition(test.source)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseDockerDefinition() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestDockerRunArgumentsAreDeterministicAndHardened(t *testing.T) {
	readOnly := true
	definition := DockerDefinition{Image: "alpine:3.22", Command: []string{"echo"}, Args: []string{"ok"}, Env: map[string]string{"B": "2", "A": "1"}, Network: "none", ReadOnlyRoot: &readOnly}
	got := dockerRunArguments("run-1", Task{RunID: "run-1", JobID: "job-1", Input: "payload"}, definition)
	wantPrefix := []string{"run", "--rm", "--name", "run-1", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "256", "--read-only"}
	if !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("arguments prefix = %#v", got)
	}
	if got[len(got)-3] != "alpine:3.22" || got[len(got)-2] != "echo" || got[len(got)-1] != "ok" {
		t.Fatalf("arguments suffix = %#v", got[len(got)-3:])
	}
}
