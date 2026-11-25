package store

import "context"

// DeadLetterEntry represents a task execution failure that exhausted retries
// and was routed to the Dead-Letter Queue (DLQ) for inspection or requeueing.
type DeadLetterEntry struct {
	ID            int64  `json:"id"`
	RunID         string `json:"run_id"`
	TaskID        string `json:"task_id"`
	WorkflowName  string `json:"workflow_name"`
	Attempt       int    `json:"attempt"`
	Reason        string `json:"reason"`
	FailedAtNs    int64  `json:"failed_at_ns"`
	Requeued      bool   `json:"requeued"`
	RequeuedRunID string `json:"requeued_run_id,omitempty"`
}

// DLQStore is an optional JournalStore extension for recording, inspecting,
// and requeueing dead-lettered task failures.
type DLQStore interface {
	RecordDeadLetter(ctx context.Context, entry DeadLetterEntry) error
	ListDeadLetters(ctx context.Context, includeRequeued bool) ([]DeadLetterEntry, error)
	MarkDeadLetterRequeued(ctx context.Context, id int64, requeuedRunID string) error
}
