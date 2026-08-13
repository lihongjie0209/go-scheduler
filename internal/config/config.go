package config

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName               string
	InstanceID                string
	HTTPAddress               string
	APIContextPath            string
	CoreHTTPAddress           string
	GRPCAddress               string
	AdvertiseGRPC             string
	AdvertiseHTTP             string
	PublicBaseURL             string
	DatabaseURL               string
	EtcdEndpoints             []string
	EtcdPrefix                string
	EtcdUsername              string
	EtcdPassword              string
	EtcdCA                    string
	EtcdCert                  string
	EtcdKey                   string
	ServiceToken              string
	PreviousToken             string
	MasterKey                 string
	MasterKeyVersion          int
	JWTSecret                 string
	CookieSecure              bool
	SMTPAddress               string
	SMTPUsername              string
	SMTPPassword              string
	SMTPFrom                  string
	GRPCTLSCert               string
	GRPCTLSKey                string
	GRPCTLSCA                 string
	GRPCTLSServerName         string
	ExecutorGRPCTLSCA         string
	ExecutorGRPCTLSServerName string
	SchedulerInterval         time.Duration
	Workers                   int
	APIDatabaseMaxConns       int32
	APIDatabaseMinConns       int32
	CoreDatabaseMaxConns      int32
	CoreDatabaseMinConns      int32
	HistoryRetention          time.Duration
}

func Load(serviceName string) (Config, error) {
	if err := validateTypedEnvironment(); err != nil {
		return Config{}, err
	}
	contextPath, err := normalizeContextPath(os.Getenv("API_CONTEXT_PATH"))
	if err != nil {
		return Config{}, err
	}
	c := Config{
		ServiceName:               serviceName,
		InstanceID:                env("INSTANCE_ID", hostname()),
		HTTPAddress:               env("HTTP_ADDRESS", ":8080"),
		APIContextPath:            contextPath,
		CoreHTTPAddress:           env("CORE_HTTP_ADDRESS", ":8081"),
		GRPCAddress:               env("GRPC_ADDRESS", ":9090"),
		AdvertiseGRPC:             env("ADVERTISE_GRPC_ADDRESS", "127.0.0.1:9090"),
		AdvertiseHTTP:             env("ADVERTISE_HTTP_ADDRESS", "http://127.0.0.1:8080"),
		PublicBaseURL:             env("PUBLIC_BASE_URL", "http://127.0.0.1:8080"),
		DatabaseURL:               os.Getenv("DATABASE_URL"),
		EtcdEndpoints:             strings.Split(env("ETCD_ENDPOINTS", "127.0.0.1:2379"), ","),
		EtcdPrefix:                env("ETCD_PREFIX", "/go-scheduler/dev/services"),
		EtcdUsername:              os.Getenv("ETCD_USERNAME"),
		EtcdPassword:              os.Getenv("ETCD_PASSWORD"),
		EtcdCA:                    os.Getenv("ETCD_CA"),
		EtcdCert:                  os.Getenv("ETCD_CERT"),
		EtcdKey:                   os.Getenv("ETCD_KEY"),
		ServiceToken:              os.Getenv("SERVICE_TOKEN"),
		PreviousToken:             os.Getenv("PREVIOUS_SERVICE_TOKEN"),
		MasterKey:                 os.Getenv("MASTER_KEY"),
		MasterKeyVersion:          integer("MASTER_KEY_VERSION", 1),
		JWTSecret:                 os.Getenv("JWT_SECRET"),
		CookieSecure:              boolean("COOKIE_SECURE", true),
		SMTPAddress:               os.Getenv("SMTP_ADDRESS"),
		SMTPUsername:              os.Getenv("SMTP_USERNAME"),
		SMTPPassword:              os.Getenv("SMTP_PASSWORD"),
		SMTPFrom:                  os.Getenv("SMTP_FROM"),
		GRPCTLSCert:               os.Getenv("GRPC_TLS_CERT"),
		GRPCTLSKey:                os.Getenv("GRPC_TLS_KEY"),
		GRPCTLSCA:                 os.Getenv("GRPC_TLS_CA"),
		GRPCTLSServerName:         os.Getenv("GRPC_TLS_SERVER_NAME"),
		ExecutorGRPCTLSCA:         os.Getenv("EXECUTOR_GRPC_TLS_CA"),
		ExecutorGRPCTLSServerName: os.Getenv("EXECUTOR_GRPC_TLS_SERVER_NAME"),
		SchedulerInterval:         duration("SCHEDULER_INTERVAL", time.Second),
		Workers:                   integer("WORKERS", 64),
		APIDatabaseMaxConns:       integer32("API_DATABASE_MAX_CONNS", 8),
		APIDatabaseMinConns:       integer32("API_DATABASE_MIN_CONNS", 1),
		CoreDatabaseMaxConns:      integer32("CORE_DATABASE_MAX_CONNS", 24),
		CoreDatabaseMinConns:      integer32("CORE_DATABASE_MIN_CONNS", 2),
		HistoryRetention:          duration("HISTORY_RETENTION", 90*24*time.Hour),
	}
	c.PublicBaseURL = appendContextPath(c.PublicBaseURL, c.APIContextPath)
	c.AdvertiseHTTP = appendContextPath(c.AdvertiseHTTP, c.APIContextPath)
	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if c.ServiceToken == "" {
		return Config{}, fmt.Errorf("SERVICE_TOKEN is required")
	}
	if c.MasterKey == "" {
		return Config{}, fmt.Errorf("MASTER_KEY is required")
	}
	if len(c.JWTSecret) < 32 {
		return Config{}, fmt.Errorf("JWT_SECRET must contain at least 32 bytes")
	}
	if c.HistoryRetention <= 0 || c.HistoryRetention > 90*24*time.Hour {
		return Config{}, fmt.Errorf("HISTORY_RETENTION must be positive and at most 2160h")
	}
	if c.SchedulerInterval <= 0 {
		return Config{}, fmt.Errorf("SCHEDULER_INTERVAL must be positive")
	}
	if c.Workers <= 0 {
		return Config{}, fmt.Errorf("WORKERS must be positive")
	}
	if err := validatePoolSize("API", c.APIDatabaseMinConns, c.APIDatabaseMaxConns); err != nil {
		return Config{}, err
	}
	if err := validatePoolSize("CORE", c.CoreDatabaseMinConns, c.CoreDatabaseMaxConns); err != nil {
		return Config{}, err
	}
	if serviceName != "scheduler-server" && (c.EtcdCert == "") != (c.EtcdKey == "") {
		return Config{}, fmt.Errorf("ETCD_CERT and ETCD_KEY must be configured together")
	}
	return c, nil
}

func validateTypedEnvironment() error {
	for _, key := range []string{"MASTER_KEY_VERSION", "WORKERS", "API_DATABASE_MAX_CONNS", "API_DATABASE_MIN_CONNS", "CORE_DATABASE_MAX_CONNS", "CORE_DATABASE_MIN_CONNS"} {
		value := os.Getenv(key)
		if value == "" {
			continue
		}
		if _, err := strconv.ParseInt(value, 10, 32); err != nil {
			return fmt.Errorf("%s must be an integer: %w", key, err)
		}
	}
	for _, key := range []string{"SCHEDULER_INTERVAL", "HISTORY_RETENTION"} {
		value := os.Getenv(key)
		if value == "" {
			continue
		}
		if _, err := time.ParseDuration(value); err != nil {
			return fmt.Errorf("%s must be a duration: %w", key, err)
		}
	}
	if value := os.Getenv("COOKIE_SECURE"); value != "" {
		if _, err := strconv.ParseBool(value); err != nil {
			return fmt.Errorf("COOKIE_SECURE must be a boolean: %w", err)
		}
	}
	return nil
}

func validatePoolSize(name string, minConns, maxConns int32) error {
	if maxConns < 1 || maxConns > 10_000 || minConns < 0 || minConns > maxConns {
		return fmt.Errorf("%s database pool requires 1 <= max <= 10000 and 0 <= min <= max", name)
	}
	return nil
}

func normalizeContextPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "/" {
		return "", nil
	}
	if strings.ContainsAny(value, "?#\\") {
		return "", fmt.Errorf("API_CONTEXT_PATH must be a URL path without query, fragment, or backslash")
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	value = strings.TrimRight(value, "/")
	cleaned := path.Clean(value)
	if cleaned != value || strings.Contains(value, "//") {
		return "", fmt.Errorf("API_CONTEXT_PATH must be a clean URL path")
	}
	return cleaned, nil
}

func appendContextPath(baseURL, contextPath string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	if contextPath == "" || strings.HasSuffix(baseURL, contextPath) {
		return baseURL
	}
	return baseURL + contextPath
}
func boolean(key string, fallback bool) bool {
	v, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
func duration(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
func integer(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}

func integer32(key string, fallback int32) int32 {
	v, err := strconv.ParseInt(os.Getenv(key), 10, 32)
	if err != nil {
		return fallback
	}
	return int32(v) // #nosec G115 -- ParseInt with bitSize 32 guarantees the value is in range.
}
