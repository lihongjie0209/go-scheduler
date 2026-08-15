package store

import (
	"strings"
	"testing"
)

func TestEnqueueDueJobsQueryOmitsScriptSource(t *testing.T) {
	t.Parallel()
	query := enqueueDueJobsQuery()
	if strings.Contains(query, "script_source") {
		t.Fatalf("enqueue query must not select script_source: %s", query)
	}
	if !strings.Contains(query, "''::text") {
		t.Fatalf("enqueue query should substitute empty script source: %s", query)
	}
	if !strings.Contains(query, "LIMIT $1") {
		t.Fatalf("enqueue query must stay bounded: %s", query)
	}
}

func TestExpireCallbacksQueryIsBounded(t *testing.T) {
	t.Parallel()
	if callbackExpiryBatchSize < 1 || callbackExpiryBatchSize > historyCleanupBatchSize {
		t.Fatalf("callbackExpiryBatchSize = %d", callbackExpiryBatchSize)
	}
	query := expireCallbacksQuery()
	if !strings.Contains(query, "LIMIT $1") {
		t.Fatalf("expire callbacks query is unbounded: %s", query)
	}
	if !strings.Contains(query, "waiting_callback") {
		t.Fatalf("expire callbacks query lost its predicate: %s", query)
	}
}
