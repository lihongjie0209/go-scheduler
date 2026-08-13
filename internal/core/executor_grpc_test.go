package core

import (
	"fmt"
	"testing"
)

func TestExecutorGRPCPoolEvictsIdleConnections(t *testing.T) {
	t.Parallel()
	pool := newExecutorGRPCPool("token")
	defer pool.close()
	for index := range maxExecutorGRPCConnections + 20 {
		_, release, err := pool.acquire(fmt.Sprintf("127.0.0.1:%d", 10000+index))
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if got := len(pool.conns); got != maxExecutorGRPCConnections {
		t.Fatalf("connection count = %d, want %d", got, maxExecutorGRPCConnections)
	}
}

func TestExecutorGRPCPoolDoesNotEvictActiveConnections(t *testing.T) {
	t.Parallel()
	pool := newExecutorGRPCPool("token")
	defer pool.close()
	releases := make([]func(), 0, maxExecutorGRPCConnections)
	for index := range maxExecutorGRPCConnections {
		_, release, err := pool.acquire(fmt.Sprintf("127.0.0.1:%d", 10000+index))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if _, _, err := pool.acquire("127.0.0.1:65535"); err == nil {
		t.Fatal("pool accepted a connection while all entries were active")
	}
	for _, release := range releases {
		release()
	}
}
