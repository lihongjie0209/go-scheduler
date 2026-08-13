package main

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
