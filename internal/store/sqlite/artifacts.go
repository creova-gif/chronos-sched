package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/creova-gif/chronos-sched/internal/store"
)

const artifactsSchemaSQL = `
CREATE TABLE IF NOT EXISTS task_artifacts (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id        TEXT NOT NULL,
	task_id       TEXT NOT NULL,
	name          TEXT NOT NULL,
	content_type  TEXT NOT NULL,
	sha256        TEXT NOT NULL,
	size_bytes    INTEGER NOT NULL,
	data          BLOB NOT NULL,
	created_at_ns INTEGER NOT NULL,
	UNIQUE(run_id, task_id, name)
);
CREATE INDEX IF NOT EXISTS idx_task_artifacts_run ON task_artifacts(run_id, task_id);
`

var _ store.ArtifactStore = (*Store)(nil)

func (s *Store) ensureArtifactsSchema(ctx context.Context) error {
	if _, err := s.writer.ExecContext(ctx, artifactsSchemaSQL); err != nil {
		return fmt.Errorf("init task_artifacts schema: %w", err)
	}
	return nil
}

// PutArtifact validates, hashes, and upserts a task artifact, returning the stored record with its ID.
func (s *Store) PutArtifact(ctx context.Context, art store.ArtifactRecord) (store.ArtifactRecord, error) {
	if s.readOnly {
		return store.ArtifactRecord{}, store.ErrStoreReadOnly
	}
	if art.RunID == "" {
		return store.ArtifactRecord{}, fmt.Errorf("put artifact: run_id is required")
	}
	if art.TaskID == "" {
		return store.ArtifactRecord{}, fmt.Errorf("put artifact: task_id is required")
	}
	if art.Name == "" {
		return store.ArtifactRecord{}, fmt.Errorf("put artifact: name is required")
	}

	if err := s.ensureArtifactsSchema(ctx); err != nil {
		return store.ArtifactRecord{}, err
	}

	data := art.Data
	if data == nil {
		data = []byte{}
	}

	sum := sha256.Sum256(data)
	art.SHA256 = hex.EncodeToString(sum[:])
	art.SizeBytes = int64(len(data))
	if art.ContentType == "" {
		art.ContentType = "application/octet-stream"
	}
	if art.CreatedAtNs <= 0 {
		art.CreatedAtNs = time.Now().UnixNano()
	}

	err := s.writer.QueryRowContext(ctx, `
		INSERT INTO task_artifacts (run_id, task_id, name, content_type, sha256, size_bytes, data, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id, task_id, name) DO UPDATE SET
			content_type  = excluded.content_type,
			sha256        = excluded.sha256,
			size_bytes    = excluded.size_bytes,
			data          = excluded.data,
			created_at_ns = excluded.created_at_ns
		RETURNING id`,
		art.RunID,
		art.TaskID,
		art.Name,
		art.ContentType,
		art.SHA256,
		art.SizeBytes,
		data,
		art.CreatedAtNs,
	).Scan(&art.ID)
	if err != nil {
		return store.ArtifactRecord{}, fmt.Errorf("upsert artifact %q/%q/%q: %w", art.RunID, art.TaskID, art.Name, err)
	}

	return art, nil
}

// GetArtifact retrieves a full artifact record including its Data payload.
// Returns sql.ErrNoRows (wrapped) if the artifact does not exist or the table has not been created yet.
func (s *Store) GetArtifact(ctx context.Context, runID, taskID, name string) (store.ArtifactRecord, error) {
	var art store.ArtifactRecord
	err := s.reader.QueryRowContext(ctx, `
		SELECT id, run_id, task_id, name, content_type, sha256, size_bytes, data, created_at_ns
		FROM task_artifacts
		WHERE run_id = ? AND task_id = ? AND name = ?`,
		runID, taskID, name,
	).Scan(
		&art.ID,
		&art.RunID,
		&art.TaskID,
		&art.Name,
		&art.ContentType,
		&art.SHA256,
		&art.SizeBytes,
		&art.Data,
		&art.CreatedAtNs,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table") {
			return store.ArtifactRecord{}, fmt.Errorf("artifact %q/%q/%q not found: %w", runID, taskID, name, sql.ErrNoRows)
		}
		return store.ArtifactRecord{}, fmt.Errorf("query artifact %q/%q/%q: %w", runID, taskID, name, err)
	}
	return art, nil
}

// ListRunArtifacts returns metadata for all artifacts belonging to runID ordered by id ASC,
// omitting the Data blob for fast listing. If the task_artifacts table does not yet exist,
// an empty slice is returned without error.
func (s *Store) ListRunArtifacts(ctx context.Context, runID string) ([]store.ArtifactRecord, error) {
	rows, err := s.reader.QueryContext(ctx, `
		SELECT id, run_id, task_id, name, content_type, sha256, size_bytes, created_at_ns
		FROM task_artifacts
		WHERE run_id = ?
		ORDER BY id ASC`,
		runID,
	)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return []store.ArtifactRecord{}, nil
		}
		return nil, fmt.Errorf("query run artifacts for %q: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	artifacts := []store.ArtifactRecord{}
	for rows.Next() {
		var art store.ArtifactRecord
		if err := rows.Scan(
			&art.ID,
			&art.RunID,
			&art.TaskID,
			&art.Name,
			&art.ContentType,
			&art.SHA256,
			&art.SizeBytes,
			&art.CreatedAtNs,
		); err != nil {
			return nil, fmt.Errorf("scan run artifact: %w", err)
		}
		artifacts = append(artifacts, art)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run artifacts: %w", err)
	}
	return artifacts, nil
}
