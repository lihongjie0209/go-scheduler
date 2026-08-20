package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("version conflict")
var ErrQueueFull = errors.New("job queue is full")
var ErrNotCancellable = errors.New("run is not cancellable")
var ErrDependencyCycle = errors.New("job dependency would create a cycle")
var ErrRegistrationMode = errors.New("executor group does not accept dynamic registration")
var ErrExecutorGroupInUse = errors.New("executor group is referenced by a job")
var ErrOverrideRequiresExecutorGroup = errors.New("executor address override requires an executor group job")
var ErrOverrideAddressNotRegistered = errors.New("override addresses must belong to the job executor group")
var ErrNotificationLeaseLost = errors.New("notification delivery lease lost")
var ErrNotificationConfigUnreadable = errors.New("notification channel configuration is unreadable")
var ErrInvalidNotificationScope = errors.New("notification channel must target all jobs or one or more specific jobs")

type Store struct {
	pool         *pgxpool.Pool
	headerCipher HeaderCipher
}

type PoolStats struct {
	AcquiredConnections int32
	IdleConnections     int32
	TotalConnections    int32
	MaxConnections      int32
	EmptyAcquireCount   int64
	AcquireDuration     time.Duration
}
type HeaderCipher interface {
	Encrypt([]byte) ([]byte, int, error)
	Decrypt([]byte, int) ([]byte, error)
}
type storeOptions struct {
	headerCipher HeaderCipher
	maxConns     int32
	minConns     int32
}

type Option func(*storeOptions)

func WithHeaderCipher(cipher HeaderCipher) Option {
	return func(options *storeOptions) { options.headerCipher = cipher }
}

func WithPoolSize(maxConns, minConns int32) Option {
	return func(options *storeOptions) {
		options.maxConns = maxConns
		options.minConns = minConns
	}
}

func New(ctx context.Context, databaseURL string, opts ...Option) (*Store, error) {
	options := storeOptions{maxConns: 32, minConns: 2}
	for _, opt := range opts {
		opt(&options)
	}
	if options.maxConns < 1 || options.minConns < 0 || options.minConns > options.maxConns {
		return nil, fmt.Errorf("invalid database pool size: min=%d max=%d", options.minConns, options.maxConns)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	config.MaxConns = options.maxConns
	config.MinConns = options.minConns
	config.MaxConnLifetime = time.Hour
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool, headerCipher: options.headerCipher}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) PoolStats() PoolStats {
	stats := s.pool.Stat()
	return PoolStats{
		AcquiredConnections: stats.AcquiredConns(),
		IdleConnections:     stats.IdleConns(),
		TotalConnections:    stats.TotalConns(),
		MaxConnections:      stats.MaxConns(),
		EmptyAcquireCount:   stats.EmptyAcquireCount(),
		AcquireDuration:     stats.AcquireDuration(),
	}
}
func (s *Store) AuthenticateAPIKey(ctx context.Context, raw string) (string, string, error) {
	hash := sha256.Sum256([]byte(raw))
	var tenantID, role string
	err := s.pool.QueryRow(ctx, `SELECT tenant_id,role FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL`, hash[:]).Scan(&tenantID, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", fmt.Errorf("authenticate api key: %w", err)
	}
	return tenantID, role, nil
}
