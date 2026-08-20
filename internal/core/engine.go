package core

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/credentials"
)

type Engine struct {
	repository       EngineRepository
	owner            string
	interval         time.Duration
	workers          int
	publicBaseURL    string
	client           *http.Client
	wg               sync.WaitGroup
	historyRetention time.Duration
	lastCleanup      time.Time
	lastRunCleanup   time.Time
	lastPartitionRun time.Time
	dispatchWake     chan struct{}
	executorGRPC     *executorGRPCPool
}

type EngineSchedulerRepository interface {
	EnqueueDue(context.Context, int) error
	ExpireCallbacks(context.Context) error
	MaintainRunPartitions(context.Context, time.Time, time.Duration) (store.PartitionMaintenanceResult, error)
	CleanupAuxiliaryHistory(context.Context, time.Duration) error
	CleanupRunHistory(context.Context, time.Duration) error
}

type EngineCommandRepository interface {
	ClaimExecutorCommands(context.Context, string, int) ([]store.ExecutorCommand, error)
	CompleteExecutorCommand(context.Context, string, string) error
	RetryExecutorCommand(context.Context, string, string, string, time.Duration) error
}

type EngineRunRepository interface {
	ClaimRuns(context.Context, string, int, time.Duration) ([]store.ClaimedRun, error)
	PrepareClaimedExecutorDispatch(context.Context, store.Run, string, string, []byte, time.Time) error
	CompleteCallback(context.Context, string, []byte, bool, string) error
	IsRunRunning(context.Context, string) (bool, error)
	FailRun(context.Context, store.Run, string, int, string, *time.Duration) (*store.Run, error)
}

type EngineRoutingRepository interface {
	JobExecutorLabels(context.Context, string) ([]string, []string, error)
	ExecutorRouteStrategy(context.Context, string, string, string) (string, error)
	ExecutorRouteCandidates(context.Context, string, string, string) (string, []store.ExecutorNode, error)
	ReserveExecutorRoute(context.Context, string, string, string, store.ExecutorSelector) (store.ExecutorNode, error)
	ReserveExecutorOverrideRoute(context.Context, string, string, string, []string, store.ExecutorSelector) (store.ExecutorNode, error)
	GetKubernetesCluster(context.Context, string, string) (store.KubernetesCluster, error)
}

type EngineRepository interface {
	EngineSchedulerRepository
	EngineCommandRepository
	EngineRunRepository
	EngineRoutingRepository
}

const auxiliaryHistoryCleanupInterval = 10 * time.Second

const (
	executorCommandBatchSize   = 16
	executorCommandConcurrency = 8
	executorCommandMaxPoll     = time.Second
	executorCommandTimeout     = 5 * time.Second
	cancelWatchInterval        = 2 * time.Second
	persistWriteTimeout        = 10 * time.Second
)

func persistContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), persistWriteTimeout)
}

func persistWrite(parent context.Context, write func(context.Context) error) error {
	ctx, cancel := persistContext(parent)
	defer cancel()
	return write(ctx)
}

type EngineOption func(*Engine)

func WithHTTPClient(client *http.Client) EngineOption {
	return func(engine *Engine) { engine.client = client }
}

func WithExecutorGRPC(token string) EngineOption {
	return func(engine *Engine) { engine.executorGRPC = newExecutorGRPCPool(token) }
}

func WithExecutorGRPCTransport(token string, transport credentials.TransportCredentials) EngineOption {
	return func(engine *Engine) { engine.executorGRPC = newExecutorGRPCPoolWithTransport(token, transport) }
}

func WithExecutorController(controller *ExecutorController) EngineOption {
	return func(engine *Engine) {
		if controller != nil {
			engine.executorGRPC = controller.pool
		}
	}
}

func NewEngine(repository EngineRepository, owner string, interval time.Duration, workers int, publicBaseURL string, historyRetention time.Duration, _ []string, options ...EngineOption) *Engine {
	engine := &Engine{repository: repository, owner: owner, interval: interval, workers: workers, publicBaseURL: strings.TrimRight(publicBaseURL, "/"), historyRetention: historyRetention, dispatchWake: make(chan struct{}, 1)}
	for _, option := range options {
		option(engine)
	}
	if engine.client == nil {
		transport := &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second}
		engine.client = &http.Client{Transport: transport}
	}
	return engine
}

func (e *Engine) Run(ctx context.Context) {
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.loop(ctx) }()
	e.wg.Add(1)
	go func() { defer e.wg.Done(); e.maintenanceLoop(ctx) }()
	if e.executorGRPC != nil {
		e.wg.Add(1)
		go func() { defer e.wg.Done(); e.executorCommandLoop(ctx) }()
	}
}
func (e *Engine) Wait() {
	e.wg.Wait()
	if e.executorGRPC != nil {
		e.executorGRPC.close()
	}
}
func (e *Engine) loop(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	sem := make(chan struct{}, e.workers)
	if err := e.tick(ctx, sem); err != nil && ctx.Err() == nil {
		slog.Error("scheduler tick failed", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.tick(ctx, sem); err != nil && ctx.Err() == nil {
				slog.Error("scheduler tick failed", "error", err)
			}
		case <-e.dispatchWake:
			if err := e.dispatch(ctx, sem); err != nil && ctx.Err() == nil {
				slog.Error("scheduler dispatch failed", "error", err)
			}
		}
	}
}
