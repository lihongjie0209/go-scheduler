package perfbench

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestBenchmarkExecutorForwardsBothProtocols(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	received := make([]string, 0, 2)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, r.URL.Query().Get("id"))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(sink.Close)
	executor, err := NewBenchmarkExecutor(BenchmarkExecutorOptions{SinkURL: sink.URL + "/execute", XXLAccessToken: "token", XXLAppName: "benchmark", XXLHandler: "schedulerBenchmarkHandler", HTTPClient: sink.Client()})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(executor)
	t.Cleanup(server.Close)

	goResponse := postBenchmark(t, server.Client(), server.URL+"/go?id=go-event", "", nil)
	goStatus := goResponse.StatusCode
	closeBenchmarkResponse(t, goResponse)
	if goStatus != http.StatusNoContent {
		t.Fatalf("Go execution status = %d", goStatus)
	}

	xxlBody := `{"jobId":42,"executorHandler":"schedulerBenchmarkHandler","executorParams":"xxl-event","executorBlockStrategy":"SERIAL_EXECUTION","executorTimeout":10,"logId":99,"logDateTime":1,"glueType":"BEAN","broadcastIndex":0,"broadcastTotal":1}`
	xxlHTTPResponse := postBenchmark(t, server.Client(), server.URL+"/trigger", xxlBody, map[string]string{"XXL-JOB-ACCESS-TOKEN": "token", "XXL-JOB-APPNAME": "benchmark"})
	var result xxlResponse
	decodeErr := json.NewDecoder(xxlHTTPResponse.Body).Decode(&result)
	status := xxlHTTPResponse.StatusCode
	closeBenchmarkResponse(t, xxlHTTPResponse)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if status != http.StatusOK || result.Code != http.StatusOK {
		t.Fatalf("XXL response status=%d body=%+v", status, result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != "go-event" || received[1] != "xxl-event" {
		t.Fatalf("sink events = %v", received)
	}
}

func TestBenchmarkExecutorRejectsInvalidXXLAuthentication(t *testing.T) {
	t.Parallel()
	executor, err := NewBenchmarkExecutor(BenchmarkExecutorOptions{SinkURL: "https://sink.example/execute", XXLAccessToken: "token", XXLAppName: "benchmark", XXLHandler: "handler"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(executor)
	t.Cleanup(server.Close)
	response := postBenchmark(t, server.Client(), server.URL+"/trigger", `{"executorHandler":"handler","executorParams":"event"}`, nil)
	var result xxlResponse
	decodeErr := json.NewDecoder(response.Body).Decode(&result)
	status := response.StatusCode
	closeBenchmarkResponse(t, response)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if status != http.StatusOK || result.Code != http.StatusUnauthorized {
		t.Fatalf("response status=%d body=%+v", status, result)
	}
}

func postBenchmark(t *testing.T, client *http.Client, target, body string, headers map[string]string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func closeBenchmarkResponse(t *testing.T, response *http.Response) {
	t.Helper()
	if err := response.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}
