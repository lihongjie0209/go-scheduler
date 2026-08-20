package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"github.com/lihongjie0209/go-scheduler/internal/auth"
	"github.com/lihongjie0209/go-scheduler/internal/observability"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	client       schedulerv1.SchedulerServiceClient
	auth         *auth.Manager
	cookieSecure bool
	contextPath  string
	etcd         *clientv3.Client
	etcdPrefix   string
	instances    []map[string]any
	logins       *loginLimiter
}

func (s *Server) SetStandaloneInstance(instanceID string, startedAt time.Time) {
	s.instances = []map[string]any{{
		"service": "scheduler-server", "instance_id": instanceID,
		"version": "dev", "started_at": startedAt.UTC(), "draining": false,
	}}
}

func (s *Server) SetDiscovery(client *clientv3.Client, prefix string) {
	s.etcd = client
	s.etcdPrefix = prefix
}

func (s *Server) SetContextPath(contextPath string) { s.contextPath = contextPath }

func NewServer(client schedulerv1.SchedulerServiceClient, manager *auth.Manager, cookieSecure ...bool) *Server {
	secure := true
	if len(cookieSecure) > 0 {
		secure = cookieSecure[0]
	}
	return &Server{client: client, auth: manager, cookieSecure: secure, logins: newLoginLimiter()}
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.client.Ping(ctx, &schedulerv1.PingRequest{}); err != nil {
		writeError(w, 503, "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func applyDefaults(j *schedulerv1.Job) {
	if j.Timezone == "" {
		j.Timezone = "UTC"
	}
	if j.HttpMethod == "" {
		j.HttpMethod = "POST"
	}
	if j.TimeoutSeconds == 0 {
		j.TimeoutSeconds = 30
	}
	if j.OverlapPolicy == "" {
		j.OverlapPolicy = "serial"
	}
	if j.MisfirePolicy == "" {
		j.MisfirePolicy = "fire_once"
	}
	if j.MaxConcurrentRuns == 0 {
		j.MaxConcurrentRuns = 1
	}
	if j.MaxCatchUp == 0 {
		j.MaxCatchUp = 10
	}
	if j.CallbackTimeoutSeconds == 0 {
		j.CallbackTimeoutSeconds = 3600
	}
	if j.MaxQueueSize == 0 {
		j.MaxQueueSize = 1000
	}
	if j.ExecutorHandler == "" && j.TargetUrl != "" && j.ScriptLanguage == "" {
		j.ExecutorHandler = "__http__"
	}
}
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if message, ok := dst.(proto.Message); ok {
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			err = (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, message)
		}
		if err != nil {
			writeError(w, 400, "invalid JSON: "+err.Error())
			return false
		}
		return true
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, 400, "invalid JSON: request body must contain exactly one JSON value")
		return false
	}
	return true
}
func respond(w http.ResponseWriter, v any, err error, code int) {
	if err == nil {
		if message, ok := v.(proto.Message); ok {
			payload, marshalErr := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)
			if marshalErr != nil {
				writeError(w, 500, "internal error")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			if _, writeErr := w.Write(payload); writeErr != nil {
				slog.Error("write protobuf response", "error", writeErr)
			}
			return
		}
		writeJSON(w, code, v)
		return
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		writeError(w, 400, status.Convert(err).Message())
	case codes.NotFound:
		writeError(w, 404, "resource not found")
	case codes.Aborted:
		writeError(w, 409, "resource version conflict")
	case codes.FailedPrecondition:
		writeError(w, 409, status.Convert(err).Message())
	case codes.Unavailable:
		writeError(w, 503, "scheduler core unavailable")
	case codes.ResourceExhausted:
		writeError(w, http.StatusTooManyRequests, status.Convert(err).Message())
	default:
		writeError(w, 500, "internal error")
	}
}
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "error", err)
	}
}
func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("panic recovered", "panic", recovered)
				writeError(w, 500, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(recorder, r)
		observability.HTTPDuration.WithLabelValues(r.Method).Observe(time.Since(start).Seconds())
		observability.HTTPRequests.WithLabelValues(r.Method, strconv.Itoa(recorder.status/100)+"xx").Inc()
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration", time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
