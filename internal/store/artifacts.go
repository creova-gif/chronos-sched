package store

import "context"

// ArtifactRecord represents a binary or structured output artifact produced by a task run.
type ArtifactRecord struct {
	ID          int64  `json:"id"`
	RunID       string `json:"run_id"`
	TaskID      string `json:"task_id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	Data        []byte `json:"data,omitempty"`
	CreatedAtNs int64  `json:"created_at_ns"`
}

// ArtifactStore is an optional store extension for persisting and retrieving
// task execution artifacts.
type ArtifactStore interface {
	PutArtifact(ctx context.Context, art ArtifactRecord) (ArtifactRecord, error)
	GetArtifact(ctx context.Context, runID, taskID, name string) (ArtifactRecord, error)
	ListRunArtifacts(ctx context.Context, runID string) ([]ArtifactRecord, error)
}
