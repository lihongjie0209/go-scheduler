package perfbench

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

const (
	maxRequestBody   = 8 << 20
	maxExecutionBody = 1 << 20
)

type Server struct {
	collector *Collector
	now       func() time.Time
}

func NewServer(collector *Collector) http.Handler {
	server := &Server{collector: collector, now: time.Now}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /execute", server.execute)
	mux.HandleFunc("POST /api/v1/expect", server.expect)
	mux.HandleFunc("GET /api/v1/report", server.report)
	mux.HandleFunc("POST /api/v1/reset", server.reset)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (s *Server) execute(w http.ResponseWriter, r *http.Request) {
	if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxExecutionBody)); err != nil {
		s.collector.MarkInvalid()
		writeError(w, http.StatusRequestEntityTooLarge, "execution body exceeds limit")
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		id = r.Header.Get("X-Benchmark-Event-ID")
	}
	duplicate, expected := s.collector.Observe(Observation{ID: id, ReceivedAt: s.now()})
	if id == "" {
		writeError(w, http.StatusBadRequest, "event ID is required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"accepted": expected, "duplicate": duplicate})
}

func (s *Server) expect(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Events []ExpectedEvent `json:"events"`
	}
	if err := decodeJSON(w, r, &request); err != nil {
		s.collector.MarkInvalid()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.collector.Expect(request.Events); err != nil {
		s.collector.MarkInvalid()
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"expected": s.collector.Report().Expected})
}

func (s *Server) report(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.collector.Report())
}

func (s *Server) reset(w http.ResponseWriter, _ *http.Request) {
	s.collector.Reset()
	w.WriteHeader(http.StatusNoContent)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain one JSON value")
		}
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
