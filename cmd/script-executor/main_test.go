package scriptexecutor

import (
	"reflect"
	"testing"
	"time"
)

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
