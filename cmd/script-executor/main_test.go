package scriptexecutor

import (
	"reflect"
	"testing"
)

func TestSplitLanguages(t *testing.T) {
	t.Parallel()
	if got, want := splitLanguages(" shell, python ,,"), []string{"shell", "python"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
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
