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
		{name: "host network", source: `{"image":"alpine","network":"host"}`},
		{name: "custom network", source: `{"image":"alpine","network":"tenant-jobs"}`},
		{name: "invalid network", source: `{"image":"alpine","network":"bad network"}`, wantErr: true},
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

func TestDockerRunArgumentsAreDeterministic(t *testing.T) {
	readOnly := true
	definition := DockerDefinition{Image: "alpine:3.22", Command: []string{"echo"}, Args: []string{"ok"}, Env: map[string]string{"B": "2", "A": "1"}, Network: "none", ReadOnlyRoot: &readOnly}
	got := dockerRunArguments("run-1", Task{RunID: "run-1", JobID: "job-1", Input: "payload"}, definition)
	wantPrefix := []string{"run", "--name", "run-1", "--label", "go-scheduler.managed-by=lihongjie0209", "--label", "go-scheduler.run-id=run-1", "--label", "go-scheduler.job-id=job-1", "--network", "none", "--read-only"}
	if !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("arguments prefix = %#v", got)
	}
	if got[len(got)-3] != "alpine:3.22" || got[len(got)-2] != "echo" || got[len(got)-1] != "ok" {
		t.Fatalf("arguments suffix = %#v", got[len(got)-3:])
	}
}

func TestDockerRunArgumentsDefaultToDockerRuntimePolicy(t *testing.T) {
	got := dockerRunArguments("run-1", Task{RunID: "run-1", JobID: "job-1"}, DockerDefinition{Image: "alpine:3.22"})
	wantPrefix := []string{"run", "--name", "run-1", "--label", "go-scheduler.managed-by=lihongjie0209", "--label", "go-scheduler.run-id=run-1", "--label", "go-scheduler.job-id=job-1"}
	if !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("arguments prefix = %#v", got)
	}
	for _, forbidden := range []string{"--network", "--cap-drop", "--security-opt", "--pids-limit", "--read-only"} {
		for _, argument := range got {
			if argument == forbidden {
				t.Fatalf("default arguments unexpectedly include %s: %#v", forbidden, got)
			}
		}
	}
}

func TestValidateManagedDockerInspection(t *testing.T) {
	t.Parallel()
	task := Task{RunID: "run-1", JobID: "job-1"}
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "matching", raw: `[{"Config":{"Labels":{"go-scheduler.managed-by":"lihongjie0209","go-scheduler.run-id":"run-1","go-scheduler.job-id":"job-1"}}}]`, ok: true},
		{name: "different run", raw: `[{"Config":{"Labels":{"go-scheduler.managed-by":"lihongjie0209","go-scheduler.run-id":"run-2","go-scheduler.job-id":"job-1"}}}]`},
		{name: "unmanaged", raw: `[{"Config":{"Labels":{}}}]`},
		{name: "malformed", raw: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateManagedDockerInspection([]byte(tt.raw), task)
			if (err == nil) != tt.ok {
				t.Fatalf("validateManagedDockerInspection() error = %v, ok = %v", err, tt.ok)
			}
		})
	}
}

func TestDockerExitStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want int
		ok   bool
	}{
		{name: "success", raw: "0\n", want: 0, ok: true},
		{name: "failure", raw: "137\n", want: 137, ok: true},
		{name: "malformed", raw: "failed"},
		{name: "out of range", raw: "256"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := dockerExitStatus([]byte(tt.raw))
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("dockerExitStatus() = %d, %v", got, err)
			}
		})
	}
}
