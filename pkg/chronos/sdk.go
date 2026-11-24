package chronos

import (
	"context"
	"time"

	"github.com/creova-gif/chronos-sched/internal/runtime"
)

// HandlerFunc executes a single task payload and returns structured output.
type HandlerFunc = runtime.HandlerFunc

// WorkerConfig configures a custom Go worker process.
type WorkerConfig struct {
	WorkerID          string
	CoordinatorURL    string
	Concurrency       int
	HeartbeatInterval time.Duration
}

// Worker wraps the ChronosSched worker runtime without exposing internal packages.
type Worker struct {
	inner *runtime.Worker
}

// NewWorker creates a worker pre-loaded with built-in sim, shell, and http handlers.
func NewWorker(cfg WorkerConfig) *Worker {
	return &Worker{
		inner: runtime.NewWorker(runtime.WorkerConfig{
			WorkerID:          cfg.WorkerID,
			CoordinatorURL:    cfg.CoordinatorURL,
			Concurrency:       cfg.Concurrency,
			HeartbeatInterval: cfg.HeartbeatInterval,
		}),
	}
}

// RegisterHandler registers a custom Go task handler by name.
func (w *Worker) RegisterHandler(name string, fn HandlerFunc) {
	w.inner.RegisterHandler(name, fn)
}

// Run starts the worker's heartbeat and execution loops until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	return w.inner.Run(ctx)
}
