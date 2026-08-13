package perfbench

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestXXLRegistrarRegistersAndRemoves(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	operations := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("XXL-JOB-ACCESS-TOKEN") != "token" || r.Header.Get("XXL-JOB-APPNAME") != "benchmark" {
			t.Errorf("registration headers = %v", r.Header)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload["registryGroup"] != "EXECUTOR" || payload["registryKey"] != "benchmark" || payload["registryValue"] != "http://executor:19100" {
			t.Errorf("registration payload = %v", payload)
		}
		mu.Lock()
		operations = append(operations, r.URL.Path)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"code":200,"msg":null,"data":null}`))
	}))
	t.Cleanup(server.Close)
	registrar, err := NewXXLRegistrar(XXLRegistrarOptions{AdminURL: server.URL + "/xxl-job-admin", AccessToken: "token", AppName: "benchmark", Address: "http://executor:19100/", Interval: 5 * time.Second, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- registrar.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		registered := len(operations) > 0
		mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registrar did not send initial heartbeat")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(operations) != 2 || operations[0] != "/xxl-job-admin/api/registry" || operations[1] != "/xxl-job-admin/api/registryRemove" {
		t.Fatalf("operations = %v", operations)
	}
}
