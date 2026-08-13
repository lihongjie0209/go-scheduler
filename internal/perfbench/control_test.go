package perfbench

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegisterExpectationsUsesBatchesAndControlPath(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	var received atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/expect" || r.URL.RawQuery != "" {
			t.Errorf("control target = %s", r.URL.String())
		}
		var payload struct {
			Events []ExpectedEvent `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		requests.Add(1)
		received.Add(int32(len(payload.Events)))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"expected":1}`))
	}))
	t.Cleanup(server.Close)
	events, err := BuildExpectedEvents("batch", expectationBatchSize+1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = RegisterExpectations(t.Context(), server.Client(), server.URL+"/execute?old=query", events); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || received.Load() != expectationBatchSize+1 {
		t.Fatalf("requests=%d received=%d", requests.Load(), received.Load())
	}
}

func TestBuildExpectedEvents(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 13, 12, 0, 0, 0, time.FixedZone("offset", 8*60*60))
	events, err := BuildExpectedEvents("steady", 2, at)
	if err != nil {
		t.Fatal(err)
	}
	if events[0].ID != "steady-000000" || events[1].ID != "steady-000001" || events[0].ScheduledAt.Location() != time.UTC {
		t.Fatalf("events = %+v", events)
	}
}
