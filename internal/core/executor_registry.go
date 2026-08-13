package core

import (
	"context"
	"time"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type ExecutorRegistry interface {
	RegisterExecutorNode(context.Context, string, string, string, string, time.Duration) (store.ExecutorNode, error)
	UnregisterExecutorNode(context.Context, string, string, string) error
	ListExecutorNodes(context.Context, string, string, bool) ([]store.ExecutorNode, error)
}
