package core

import (
	"strings"
	"testing"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

func TestValidateRunLogEntries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		entries []*schedulerv1.RunLogInput
		wantErr bool
	}{
		{name: "valid", entries: []*schedulerv1.RunLogInput{{EntryId: "executor-1", Stream: "stdout", Content: "started"}}},
		{name: "stderr", entries: []*schedulerv1.RunLogInput{{EntryId: "executor-2", Stream: "stderr", Content: "warning"}}},
		{name: "empty batch", wantErr: true},
		{name: "missing entry id", entries: []*schedulerv1.RunLogInput{{Stream: "stdout", Content: "x"}}, wantErr: true},
		{name: "invalid stream", entries: []*schedulerv1.RunLogInput{{EntryId: "x", Stream: "debug", Content: "x"}}, wantErr: true},
		{name: "oversized content", entries: []*schedulerv1.RunLogInput{{EntryId: "x", Stream: "stdout", Content: strings.Repeat("x", maxRunLogEntryBytes+1)}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRunLogEntries(tt.entries); (err != nil) != tt.wantErr {
				t.Fatalf("validateRunLogEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
