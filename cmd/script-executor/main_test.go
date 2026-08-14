package scriptexecutor

import (
	"reflect"
	"testing"
	"time"
)

func TestExecutorAdvertiseAddress(t *testing.T) {
	tests := []struct {
		name, explicit, host, listen, want string
		wantErr                            bool
	}{
		{name: "explicit address wins", explicit: "grpc://executor:9999", host: "10.0.0.2", listen: ":9999", want: "grpc://executor:9999"},
		{name: "derives IPv4 Pod address", host: "10.0.0.2", listen: ":9999", want: "grpc://10.0.0.2:9999"},
		{name: "derives IPv6 Pod address", host: "2001:db8::2", listen: "[::]:9999", want: "grpc://[2001:db8::2]:9999"},
		{name: "requires host or address", listen: ":9999", wantErr: true},
		{name: "requires listen port", host: "10.0.0.2", listen: "invalid", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := executorAdvertiseAddress(test.explicit, test.host, test.listen)
			if (err != nil) != test.wantErr {
				t.Fatalf("executorAdvertiseAddress() error = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("executorAdvertiseAddress() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSplitLanguages(t *testing.T) {
	t.Parallel()
	if got, want := splitLanguages(" shell, python ,,"), []string{"shell", "python"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestDurationEnv(t *testing.T) {
	t.Setenv("EXECUTOR_SHUTDOWN_TIMEOUT", "45s")
	if got, err := durationEnv("EXECUTOR_SHUTDOWN_TIMEOUT", 30*time.Second, time.Second, time.Hour); err != nil || got != 45*time.Second {
		t.Fatalf("durationEnv() = %s, %v", got, err)
	}
	t.Setenv("EXECUTOR_SHUTDOWN_TIMEOUT", "500ms")
	if _, err := durationEnv("EXECUTOR_SHUTDOWN_TIMEOUT", 30*time.Second, time.Second, time.Hour); err == nil {
		t.Fatal("too-short shutdown timeout was accepted")
	}
}

func TestPositiveIntEnv(t *testing.T) {
	t.Setenv("EXECUTOR_MAX_CONCURRENCY", "17")
	if got, err := positiveIntEnv("EXECUTOR_MAX_CONCURRENCY", 32); err != nil || got != 17 {
		t.Fatalf("positiveIntEnv() = %d, %v", got, err)
	}
	t.Setenv("EXECUTOR_MAX_CONCURRENCY", "0")
	if _, err := positiveIntEnv("EXECUTOR_MAX_CONCURRENCY", 32); err == nil {
		t.Fatal("zero concurrency was accepted")
	}
}
