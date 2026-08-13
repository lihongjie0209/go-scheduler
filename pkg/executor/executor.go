// Package executor runs HTTP, script, container, and Kubernetes handlers
// dispatched by Scheduler Core over gRPC.
package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxRunRequestBytes = 2 << 20

type Handler func(context.Context, Task) error
type TaskLogger interface {
	Info(string) error
	Error(string) error
}

type Task struct {
	RunID, JobID, Input, BroadcastGroupID, ExternalExecutionID string
	ScriptLanguage, ScriptSource                               string
	KubernetesCluster                                          *KubernetesClusterConfig
	HTTP                                                       *HTTPExecution
	BroadcastIndex, BroadcastTotal                             int32
	Logger                                                     TaskLogger
}

type HTTPExecution struct {
	URL, Method, Body string
	Headers           map[string]string
}

type Options struct {
	SchedulerURL string
	HTTPClient   *http.Client
}

type LogEntry struct {
	EntryID string `json:"entry_id"`
	Stream  string `json:"stream"`
	Content string `json:"content"`
}

type registeredHandler struct {
	handler Handler
	async   bool
}

type Server struct {
	scheduler *url.URL
	client    *http.Client
	mux       *http.ServeMux
	mu        sync.RWMutex
	handlers  map[string]registeredHandler
	active    map[string]int
}

type runRequest struct {
	RunID               string                   `json:"run_id"`
	ExternalExecutionID string                   `json:"external_execution_id,omitempty"`
	JobID               string                   `json:"job_id"`
	Handler             string                   `json:"handler"`
	Input               string                   `json:"input"`
	CallbackURL         string                   `json:"callback_url"`
	LogURL              string                   `json:"log_url"`
	CallbackToken       string                   `json:"callback_token"`
	TimeoutSeconds      int32                    `json:"timeout_seconds"`
	BroadcastGroupID    string                   `json:"broadcast_group_id"`
	BroadcastIndex      int32                    `json:"broadcast_index"`
	BroadcastTotal      int32                    `json:"broadcast_total"`
	ScriptLanguage      string                   `json:"script_language"`
	ScriptSource        string                   `json:"script_source"`
	KubernetesCluster   *KubernetesClusterConfig `json:"kubernetes_cluster,omitempty"`
}

func NewServer(options Options) (*Server, error) {
	scheduler, err := url.Parse(options.SchedulerURL)
	if err != nil || scheduler.Scheme == "" || scheduler.Host == "" {
		return nil, errors.New("scheduler URL must be absolute")
	}
	if scheduler.Scheme != "http" && scheduler.Scheme != "https" {
		return nil, errors.New("scheduler URL must use http or https")
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	s := &Server{scheduler: scheduler, client: client, mux: http.NewServeMux(), handlers: map[string]registeredHandler{}, active: map[string]int{}}
	s.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	s.mux.HandleFunc("GET /idle", s.idle)
	s.mux.HandleFunc("POST /run", s.run)
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) Handle(name string, handler Handler) error { return s.register(name, handler, false) }
func (s *Server) HandleAsync(name string, handler Handler) error {
	return s.register(name, handler, true)
}
func (s *Server) register(name string, handler Handler, async bool) error {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 128 || handler == nil {
		return errors.New("handler name and function are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.handlers[name]; exists {
		return fmt.Errorf("handler %q already registered", name)
	}
	s.handlers[name] = registeredHandler{handler: handler, async: async}
	return nil
}

func (s *Server) idle(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	s.mu.RLock()
	busy := s.active[jobID] > 0
	s.mu.RUnlock()
	if busy {
		http.Error(w, "busy", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRunRequestBytes)
	var request runRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid run request", http.StatusBadRequest)
		return
	}
	if request.RunID == "" || request.JobID == "" || request.Handler == "" || request.CallbackToken == "" || request.TimeoutSeconds < 1 || request.TimeoutSeconds > 86400 || !s.schedulerURL(request.CallbackURL) || !s.schedulerURL(request.LogURL) {
		http.Error(w, "invalid run request", http.StatusBadRequest)
		return
	}
	s.mu.RLock()
	registered, exists := s.handlers[request.Handler]
	s.mu.RUnlock()
	if !exists {
		http.Error(w, "handler not found", http.StatusNotFound)
		return
	}
	execute := func(parent context.Context) error {
		s.markActive(request.JobID, 1)
		defer s.markActive(request.JobID, -1)
		ctx, cancel := context.WithTimeout(parent, time.Duration(request.TimeoutSeconds)*time.Second)
		defer cancel()
		logger := &Logger{client: s.client, url: request.LogURL, token: request.CallbackToken}
		return invokeHandler(ctx, registered.handler, Task{RunID: request.RunID, JobID: request.JobID, Input: request.Input, BroadcastGroupID: request.BroadcastGroupID, ExternalExecutionID: request.ExternalExecutionID, BroadcastIndex: request.BroadcastIndex, BroadcastTotal: request.BroadcastTotal, ScriptLanguage: request.ScriptLanguage, ScriptSource: request.ScriptSource, KubernetesCluster: request.KubernetesCluster, Logger: logger})
	}
	if registered.async {
		go func() { err := execute(context.Background()); _ = s.callback(request, err) }()
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if err := execute(r.Context()); err != nil {
		http.Error(w, truncate(err.Error(), 4096), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func invokeHandler(ctx context.Context, handler Handler, task Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, task)
}

func (s *Server) markActive(jobID string, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[jobID] += delta
	if s.active[jobID] <= 0 {
		delete(s.active, jobID)
	}
}
func (s *Server) schedulerURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == s.scheduler.Scheme && parsed.Host == s.scheduler.Host
}
func (s *Server) callback(request runRequest, handlerErr error) error {
	payload := map[string]any{"token": request.CallbackToken, "succeeded": handlerErr == nil, "message": ""}
	if handlerErr != nil {
		payload["message"] = truncate(handlerErr.Error(), 4096)
	}
	return postJSON(context.Background(), s.client, request.CallbackURL, payload)
}

type Logger struct {
	client     *http.Client
	url, token string
}

func (l *Logger) Info(content string) error  { return l.write("stdout", content) }
func (l *Logger) Error(content string) error { return l.write("stderr", content) }
func (l *Logger) write(stream, content string) error {
	if len(content) > 65536 {
		return errors.New("log content exceeds 65536 bytes")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	payload := map[string]any{"token": l.token, "entries": []LogEntry{{EntryID: hex.EncodeToString(random[:]), Stream: stream, Content: content}}}
	return postJSON(context.Background(), l.client, l.url, payload)
}
func postJSON(ctx context.Context, client *http.Client, target string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("scheduler returned HTTP %d", response.StatusCode)
	}
	return nil
}
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
