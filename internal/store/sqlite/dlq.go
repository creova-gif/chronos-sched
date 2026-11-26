package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/store"
)

const dlqSchemaSQL = `
CREATE TABLE IF NOT EXISTS dead_letters (
	id              INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id          TEXT NOT NULL,
	task_id         TEXT NOT NULL,
	workflow_name   TEXT NOT NULL DEFAULT '',
	attempt         INTEGER NOT NULL DEFAULT 0,
	reason          TEXT NOT NULL DEFAULT '',
	failed_at_ns    INTEGER NOT NULL,
	requeued        INTEGER NOT NULL DEFAULT 0,
	requeued_run_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_requeued ON dead_letters(requeued, failed_at_ns DESC, id DESC);
`

var _ store.DLQStore = (*Store)(nil)

func (s *Store) ensureDLQSchema(ctx context.Context) error {
	if _, err := s.writer.ExecContext(ctx, dlqSchemaSQL); err != nil {
		return fmt.Errorf("init dlq schema: %w", err)
	}
	return nil
}

// RecordDeadLetter persists a dead-lettered task failure in the dead_letters table.
func (s *Store) RecordDeadLetter(ctx context.Context, entry store.DeadLetterEntry) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	if err := s.ensureDLQSchema(ctx); err != nil {
		return err
	}

	requeued := 0
	if entry.Requeued {
		requeued = 1
	}

	_, err := s.writer.ExecContext(ctx, `
		INSERT INTO dead_letters (run_id, task_id, workflow_name, attempt, reason, failed_at_ns, requeued, requeued_run_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.RunID,
		entry.TaskID,
		entry.WorkflowName,
		entry.Attempt,
		entry.Reason,
		entry.FailedAtNs,
		requeued,
		entry.RequeuedRunID,
	)
	if err != nil {
		return fmt.Errorf("insert dead letter for run %q task %q: %w", entry.RunID, entry.TaskID, err)
	}
	return nil
}

// ListDeadLetters returns dead-letter entries ordered newest-first.
// When includeRequeued is false, only active (non-requeued) entries are returned.
// If the dead_letters table does not yet exist (e.g. on a read-only database
// before any DLQ write), an empty slice is returned without error.
func (s *Store) ListDeadLetters(ctx context.Context, includeRequeued bool) ([]store.DeadLetterEntry, error) {
	query := `
		SELECT id, run_id, task_id, workflow_name, attempt, reason, failed_at_ns, requeued, requeued_run_id
		FROM dead_letters`
	if !includeRequeued {
		query += ` WHERE requeued = 0`
	}
	query += ` ORDER BY failed_at_ns DESC, id DESC`

	rows, err := s.reader.QueryContext(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return []store.DeadLetterEntry{}, nil
		}
		return nil, fmt.Errorf("query dead letters: %w", err)
	}
	defer func() { _ = rows.Close() }()

	entries := []store.DeadLetterEntry{}
	for rows.Next() {
		var (
			e        store.DeadLetterEntry
			requeued int
		)
		if err := rows.Scan(
			&e.ID,
			&e.RunID,
			&e.TaskID,
			&e.WorkflowName,
			&e.Attempt,
			&e.Reason,
			&e.FailedAtNs,
			&requeued,
			&e.RequeuedRunID,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		e.Requeued = requeued != 0
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return entries, nil
}

// MarkDeadLetterRequeued marks the dead-letter entry with the given id as requeued
// and records the newly created run ID.
func (s *Store) MarkDeadLetterRequeued(ctx context.Context, id int64, requeuedRunID string) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	if err := s.ensureDLQSchema(ctx); err != nil {
		return err
	}

	res, err := s.writer.ExecContext(ctx, `
		UPDATE dead_letters
		SET requeued = 1, requeued_run_id = ?
		WHERE id = ?`,
		requeuedRunID,
		id,
	)
	if err != nil {
		return fmt.Errorf("mark dead letter %d requeued: %w", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("dead letter %d rows affected: %w", id, err)
	}
	if affected == 0 {
		return fmt.Errorf("dead letter %d not found: %w", id, sql.ErrNoRows)
	}
	return nil
}
