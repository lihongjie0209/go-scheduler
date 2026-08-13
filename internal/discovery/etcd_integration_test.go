//go:build integration

package discovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	waitpkg "github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdRegistrationLease(t *testing.T) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{Image: "quay.io/coreos/etcd:v3.5.17", ExposedPorts: []string{"2379/tcp"}, Cmd: []string{"/usr/local/bin/etcd", "--advertise-client-urls=http://0.0.0.0:2379", "--listen-client-urls=http://0.0.0.0:2379"}, WaitingFor: waitpkg.ForListeningPort("2379/tcp")}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "2379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient([]string{fmt.Sprintf("%s:%s", host, port.Port())}, "", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registrar, err := NewRegistrar(client, "/test/services", "scheduler-core", Metadata{InstanceID: "core-1", GRPCAddress: "127.0.0.1:9090"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- registrar.Run(runCtx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, getErr := client.Get(ctx, "/test/services/scheduler-core/core-1")
		if getErr == nil && len(resp.Kvs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registration was not published")
		}
		time.Sleep(50 * time.Millisecond)
	}
	second, err := NewRegistrar(client, "/test/services", "scheduler-core", Metadata{InstanceID: "core-2", GRPCAddress: "127.0.0.1:9091"})
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, cancelSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	deadline = time.Now().Add(10 * time.Second)
	for {
		resp, getErr := client.Get(ctx, "/test/services/scheduler-core/", clientv3.WithPrefix())
		if getErr == nil && len(resp.Kvs) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second registration was not published")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("registrar did not stop")
	}
	closeCtx, closeCancel := context.WithTimeout(ctx, 3*time.Second)
	defer closeCancel()
	if err = registrar.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(ctx, "/test/services/scheduler-core/core-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatal("lease key remained after revoke")
	}
	resp, err = client.Get(ctx, "/test/services/scheduler-core/core-2")
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("closing first registrar removed second: %v, %d", err, len(resp.Kvs))
	}
	cancelSecond()
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("second registrar did not stop")
	}
	if err = second.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}
