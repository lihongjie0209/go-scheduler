package core

import (
	"crypto/md5" // #nosec G501 -- protocol-compatible non-cryptographic hash used by XXL-JOB routing.
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"time"
)

type executorCandidate struct {
	ID         string
	Address    string
	UseCount   int64
	LastUsedAt time.Time
}

func selectExecutorNode(nodes []executorCandidate, strategy string, cursor int64, random uint64, routeKey string) (executorCandidate, error) {
	if len(nodes) == 0 {
		return executorCandidate{}, fmt.Errorf("no live executor nodes")
	}
	sorted := append([]executorCandidate(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	switch strategy {
	case "first":
		return sorted[0], nil
	case "last":
		return sorted[len(sorted)-1], nil
	case "round":
		if cursor < 0 {
			cursor = 0
		}
		return sorted[int(cursor%int64(len(sorted)))], nil
	case "random":
		return sorted[int(random%uint64(len(sorted)))], nil // #nosec G115 -- modulo result is strictly less than len(sorted).
	case "hash":
		return consistentHashExecutor(sorted, routeKey), nil
	case "lfu":
		return *sortAndFirst(sorted, func(left, right executorCandidate) bool {
			return left.UseCount < right.UseCount || left.UseCount == right.UseCount && left.ID < right.ID
		}), nil
	case "lru":
		return *sortAndFirst(sorted, func(left, right executorCandidate) bool {
			if left.LastUsedAt.Equal(right.LastUsedAt) {
				return left.ID < right.ID
			}
			return left.LastUsedAt.Before(right.LastUsedAt)
		}), nil
	default:
		return executorCandidate{}, fmt.Errorf("unsupported executor route strategy %q", strategy)
	}
}

func sortAndFirst(nodes []executorCandidate, less func(executorCandidate, executorCandidate) bool) *executorCandidate {
	sort.Slice(nodes, func(i, j int) bool { return less(nodes[i], nodes[j]) })
	return &nodes[0]
}

func xxlRouteHash(key string) uint32 {
	digest := md5.Sum([]byte(key)) // #nosec G401 -- compatibility hash, not a security primitive.
	return binary.LittleEndian.Uint32(digest[:4])
}

func consistentHashExecutor(nodes []executorCandidate, routeKey string) executorCandidate {
	type virtualNode struct {
		hash uint32
		node executorCandidate
	}
	ring := make([]virtualNode, 0, len(nodes)*100)
	for _, node := range nodes {
		for index := range 100 {
			key := "SHARD-" + node.Address + "-NODE-" + strconv.Itoa(index)
			ring = append(ring, virtualNode{hash: xxlRouteHash(key), node: node})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })
	jobHash := xxlRouteHash(routeKey)
	index := sort.Search(len(ring), func(index int) bool { return ring[index].hash >= jobHash })
	if index == len(ring) {
		index = 0
	}
	return ring[index].node
}
