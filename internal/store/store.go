package store

import (
	"context"
	"errors"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

var (
	ErrRunNotFound   = errors.New("run not found")
	ErrSeqConflict   = errors.New("event sequence conflict")
	ErrStoreReadOnly = errors.New("store opened in read-only mode")
)

// RunRecord stores durable lineage and workflow metadata for a run.
type RunRecord struct {
	RunID        string           `json:"run_id"`
	ParentRunID  string           `json:"parent_run_id,omitempty"`
	ForkFromSeq  int64            `json:"fork_from_seq,omitempty"`
	WorkflowName string           `json:"workflow_name"`
	WorkflowHash string           `json:"workflow_hash"`
	WorkflowYAML string           `json:"workflow_yaml"`
	Status       engine.RunStatus `json:"status"`
	CreatedAtNs  int64            `json:"created_at_ns"`
}

// JournalStore is the pluggable durable storage seam for ChronosSched.
// Milestone 1 ships with a pure-Go SQLite WAL dual-pool adapter; Milestone 2
// can swap in replicated or custom storage engines behind this interface.
type JournalStore interface {
	CreateRun(ctx context.Context, rec RunRecord, initialEvents []engine.Event) error
	Append(ctx context.Context, runID string, expectedLastSeq int64, events []engine.Event, newStatus engine.RunStatus) error
	GetRun(ctx context.Context, runID string) (RunRecord, error)
	ListRuns(ctx context.Context, statuses ...engine.RunStatus) ([]RunRecord, error)
	// LoadRunEvents recursively resolves parent lineage prefixes (1..fork_from_seq)
	// across single- or multi-hop forks and returns one unified ordered event stream.
	LoadRunEvents(ctx context.Context, runID string) ([]engine.Event, error)
	Close() error
}
