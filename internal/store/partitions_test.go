package store

import (
	"testing"
	"time"
)

func TestRequiredRunPartitionsCoversRetentionAndPremake(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	partitions := requiredRunPartitions(now, 90*24*time.Hour, 3)
	if partitions[0].Name != "job_runs_y2026m05" {
		t.Fatalf("first partition = %s", partitions[0].Name)
	}
	if got := partitions[len(partitions)-1].Name; got != "job_runs_y2026m11" {
		t.Fatalf("last partition = %s", got)
	}
}

func TestParseRunPartitionEnd(t *testing.T) {
	end, ok := parseRunPartitionEnd("job_runs_y2026m12")
	if !ok || !end.Equal(time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("end = %s, valid = %v", end, ok)
	}
	if _, ok = parseRunPartitionEnd("job_runs_default"); ok {
		t.Fatal("default partition parsed as monthly")
	}
}
