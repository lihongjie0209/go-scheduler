package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func selectActiveExecutor(ctx context.Context, client *http.Client, strategy, jobID string, nodes []executorCandidate, probeTimeout time.Duration) (executorCandidate, error) {
	if len(nodes) == 0 {
		return executorCandidate{}, fmt.Errorf("no live executor nodes")
	}
	sorted := append([]executorCandidate(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for _, node := range sorted {
		if ctx.Err() != nil {
			return executorCandidate{}, ctx.Err()
		}
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		healthy := probeExecutor(probeCtx, client, strategy, jobID, node.Address)
		cancel()
		if healthy {
			return node, nil
		}
	}
	return executorCandidate{}, fmt.Errorf("no %s executor available", strategy)
}

func probeExecutor(ctx context.Context, client *http.Client, strategy, jobID, address string) bool {
	method := http.MethodGet
	path := "/health"
	var body io.Reader
	if strategy == "busyover" {
		method = http.MethodPost
		path = "/idle"
		payload, err := json.Marshal(map[string]string{"job_id": jobID})
		if err != nil {
			return false
		}
		body = bytes.NewReader(payload)
	} else if strategy != "failover" {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(address, "/")+path, body)
	if err != nil {
		return false
	}
	if strategy == "busyover" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Job-ID", jobID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}
