package perfbench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type BenchmarkExecutorOptions struct {
	SinkURL        string
	XXLAccessToken string
	XXLAppName     string
	XXLHandler     string
	HTTPClient     *http.Client
}

type BenchmarkExecutor struct {
	sinkURL    string
	xxlToken   string
	xxlAppName string
	xxlHandler string
	client     *http.Client
	mux        *http.ServeMux
}

type xxlTriggerRequest struct {
	ExecutorHandler string `json:"executorHandler"`
	ExecutorParams  string `json:"executorParams"`
}

func NewBenchmarkExecutor(options BenchmarkExecutorOptions) (*BenchmarkExecutor, error) {
	sink, err := url.Parse(options.SinkURL)
	if err != nil || (sink.Scheme != "http" && sink.Scheme != "https") || sink.Host == "" {
		return nil, fmt.Errorf("sink URL must be absolute HTTP or HTTPS")
	}
	if options.XXLAccessToken == "" || options.XXLAppName == "" || options.XXLHandler == "" {
		return nil, fmt.Errorf("xxl-job access token, app name, and handler are required")
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	executor := &BenchmarkExecutor{sinkURL: options.SinkURL, xxlToken: options.XXLAccessToken, xxlAppName: options.XXLAppName, xxlHandler: options.XXLHandler, client: client, mux: http.NewServeMux()}
	executor.mux.HandleFunc("POST /go", executor.executeGo)
	executor.mux.HandleFunc("POST /trigger", executor.executeXXL)
	executor.mux.HandleFunc("POST /beat", executor.xxlBeat)
	executor.mux.HandleFunc("POST /idleBeat", executor.xxlBeat)
	executor.mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return executor, nil
}

func (e *BenchmarkExecutor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.mux.ServeHTTP(w, r)
}

func (e *BenchmarkExecutor) executeGo(w http.ResponseWriter, r *http.Request) {
	if _, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxExecutionBody)); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "execution body exceeds limit")
		return
	}
	eventID := r.URL.Query().Get("id")
	if err := validateEventID(eventID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := e.forward(r.Context(), eventID); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (e *BenchmarkExecutor) executeXXL(w http.ResponseWriter, r *http.Request) {
	if !e.authenticateXXL(r) {
		writeXXLResponse(w, http.StatusUnauthorized, "invalid access token or app name")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxExecutionBody)
	decoder := json.NewDecoder(r.Body)
	var request xxlTriggerRequest
	if err := decoder.Decode(&request); err != nil {
		writeXXLResponse(w, http.StatusBadRequest, "invalid trigger request")
		return
	}
	if request.ExecutorHandler != e.xxlHandler {
		writeXXLResponse(w, http.StatusBadRequest, "benchmark handler not found")
		return
	}
	if err := validateEventID(request.ExecutorParams); err != nil {
		writeXXLResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := e.forward(r.Context(), request.ExecutorParams); err != nil {
		writeXXLResponse(w, http.StatusBadGateway, err.Error())
		return
	}
	writeXXLResponse(w, http.StatusOK, "")
}

func (e *BenchmarkExecutor) xxlBeat(w http.ResponseWriter, r *http.Request) {
	if !e.authenticateXXL(r) {
		writeXXLResponse(w, http.StatusUnauthorized, "invalid access token or app name")
		return
	}
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxExecutionBody))
	writeXXLResponse(w, http.StatusOK, "")
}

func (e *BenchmarkExecutor) authenticateXXL(r *http.Request) bool {
	return r.Header.Get("XXL-JOB-ACCESS-TOKEN") == e.xxlToken && r.Header.Get("XXL-JOB-APPNAME") == e.xxlAppName
}

func (e *BenchmarkExecutor) forward(ctx context.Context, eventID string) error {
	target, err := ExecutionURL(e.sinkURL, eventID)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
	if err != nil {
		return err
	}
	response, err := e.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("benchmark sink returned HTTP %d", response.StatusCode)
	}
	return nil
}

func validateEventID(eventID string) error {
	if strings.TrimSpace(eventID) == "" || len(eventID) > maxEventIDLength {
		return fmt.Errorf("event ID is required and must not exceed %d bytes", maxEventIDLength)
	}
	return nil
}

func writeXXLResponse(w http.ResponseWriter, code int, message string) {
	writeJSON(w, http.StatusOK, map[string]any{"code": code, "msg": message, "data": nil})
}
