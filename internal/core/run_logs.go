package core

import (
	"context"
	"fmt"
	"strings"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxRunLogEntryBytes = 64 << 10

func validateRunLogEntries(entries []*schedulerv1.RunLogInput) error {
	if len(entries) == 0 || len(entries) > 100 {
		return fmt.Errorf("entries must contain between 1 and 100 items")
	}
	for _, entry := range entries {
		if entry == nil || strings.TrimSpace(entry.EntryId) == "" || len(entry.EntryId) > 128 {
			return fmt.Errorf("entry_id must contain between 1 and 128 characters")
		}
		if entry.Stream != "stdout" && entry.Stream != "stderr" {
			return fmt.Errorf("stream must be stdout or stderr")
		}
		if len(entry.Content) > maxRunLogEntryBytes {
			return fmt.Errorf("content must not exceed %d bytes", maxRunLogEntryBytes)
		}
	}
	return nil
}

func (s *Service) AppendRunLogs(ctx context.Context, req *schedulerv1.AppendRunLogsRequest) (*schedulerv1.AppendRunLogsResponse, error) {
	entries := make([]store.RunLogInput, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, store.RunLogInput{EntryID: entry.EntryId, Stream: entry.Stream, Content: entry.Content})
	}
	cursor, err := s.operations.AppendRunLogs(ctx, req.GetRunId(), req.GetToken(), entries)
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.AppendRunLogsResponse{NextCursor: cursor}, nil
}

func (s *Service) ListRunLogs(ctx context.Context, req *schedulerv1.ListRunLogsRequest) (*schedulerv1.ListRunLogsResponse, error) {
	entries, cursor, err := s.operations.ListRunLogs(ctx, req.GetTenantId(), req.GetRunId(), req.GetAfterCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := &schedulerv1.ListRunLogsResponse{Entries: make([]*schedulerv1.RunLogEntry, 0, len(entries)), NextCursor: cursor}
	for _, entry := range entries {
		out.Entries = append(out.Entries, &schedulerv1.RunLogEntry{Cursor: entry.Cursor, EntryId: entry.EntryID, Stream: entry.Stream, Content: entry.Content, CreatedAt: timestamppb.New(entry.CreatedAt)})
	}
	return out, nil
}
