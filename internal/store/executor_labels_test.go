package store

import (
	"strings"
	"testing"
)

func TestFilterExecutorNodesRequiresAndExcludesLabels(t *testing.T) {
	t.Parallel()
	nodes := []ExecutorNode{
		{NodeID: "linux-gpu", Labels: []string{"linux", "gpu"}},
		{NodeID: "linux-cpu", Labels: []string{"linux", "cpu"}},
		{NodeID: "windows-gpu", Labels: []string{"windows", "gpu"}},
	}
	filtered := FilterExecutorNodes(nodes, []string{"linux"}, []string{"gpu"})
	if len(filtered) != 1 || filtered[0].NodeID != "linux-cpu" {
		t.Fatalf("filtered nodes = %+v", filtered)
	}
}

func TestJobExecutorLabelsByIDsQueryIsBatched(t *testing.T) {
	t.Parallel()
	query := jobExecutorLabelsByIDsQuery()
	if !strings.Contains(query, "ANY($1") {
		t.Fatalf("label query is not batched: %s", query)
	}
	if strings.Count(query, "job_id=$") > 0 {
		t.Fatalf("label query still looks per-row: %s", query)
	}
}

func TestApplyJobExecutorLabelsBatchesByJobID(t *testing.T) {
	t.Parallel()
	jobs := []Job{{ID: "job-a"}, {ID: "job-b"}, {ID: "job-c"}}
	applyJobExecutorLabels(jobs, map[string][]string{"job-a": {"linux"}, "job-c": {"gpu"}}, map[string][]string{"job-a": {"windows"}, "job-b": {"canary"}})
	if got := strings.Join(jobs[0].RequiredExecutorLabels, ","); got != "linux" {
		t.Fatalf("job-a required = %v", jobs[0].RequiredExecutorLabels)
	}
	if got := strings.Join(jobs[0].ExcludedExecutorLabels, ","); got != "windows" {
		t.Fatalf("job-a excluded = %v", jobs[0].ExcludedExecutorLabels)
	}
	if got := strings.Join(jobs[1].ExcludedExecutorLabels, ","); got != "canary" {
		t.Fatalf("job-b excluded = %v", jobs[1].ExcludedExecutorLabels)
	}
	if jobs[2].RequiredExecutorLabels[0] != "gpu" || len(jobs[1].RequiredExecutorLabels) != 0 {
		t.Fatalf("jobs = %+v", jobs)
	}
}
