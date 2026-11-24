package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

const maxLineageDepth = 128

const schemaSQL = `
CREATE TABLE IF NOT EXISTS runs (
	run_id        TEXT PRIMARY KEY,
	parent_run_id TEXT NOT NULL DEFAULT '',
	fork_from_seq INTEGER NOT NULL DEFAULT 0,
	workflow_name TEXT NOT NULL,
	workflow_hash TEXT NOT NULL,
	workflow_yaml TEXT NOT NULL,
	status        TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
	run_id        TEXT NOT NULL,
	seq           INTEGER NOT NULL,
	ts_ns         INTEGER NOT NULL,
	event_type    TEXT NOT NULL,
	task_id       TEXT NOT NULL DEFAULT '',
	attempt       INTEGER NOT NULL DEFAULT 0,
	fencing_token INTEGER NOT NULL DEFAULT 0,
	worker_id     TEXT NOT NULL DEFAULT '',
	payload_json  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (run_id, seq),
	FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_events_run_cp ON events(run_id, event_type, seq);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
`

// Store implements store.JournalStore using a dual-pool pure-Go SQLite WAL setup.
type Store struct {
	writer   *sql.DB
	reader   *sql.DB
	readOnly bool
}

// Open initializes a read-write SQLite WAL journal store with a dedicated
// single-connection writer pool and a separate 4-connection read-only pool.
func Open(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite parent dir: %w", err)
	}

	writerDSN := buildDSN(dbPath, false)
	writer, err := sql.Open("sqlite", writerDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlite writer: %w", err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	writer.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := writer.PingContext(ctx); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("ping sqlite writer: %w", err)
	}
	if _, err := writer.ExecContext(ctx, schemaSQL); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("init sqlite schema: %w", err)
	}

	readerDSN := buildDSN(dbPath, true)
	reader, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}
	reader.SetMaxOpenConns(4)
	reader.SetMaxIdleConns(4)
	reader.SetConnMaxLifetime(time.Hour)

	if err := reader.PingContext(ctx); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, fmt.Errorf("ping sqlite reader: %w", err)
	}

	return &Store{writer: writer, reader: reader, readOnly: false}, nil
}

// OpenReadOnly opens an existing SQLite WAL journal strictly in read-only mode
// for offline history, replay, and diff commands.
func OpenReadOnly(dbPath string) (*Store, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("stat sqlite db %q: %w", dbPath, err)
	}
	readerDSN := buildDSN(dbPath, true)
	reader, err := sql.Open("sqlite", readerDSN)
	if err != nil {
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}
	reader.SetMaxOpenConns(4)
	reader.SetMaxIdleConns(4)
	reader.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reader.PingContext(ctx); err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("ping sqlite reader: %w", err)
	}
	return &Store{reader: reader, readOnly: true}, nil
}

func buildDSN(dbPath string, readOnly bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(2000)")
	q.Add("_pragma", "foreign_keys(1)")
	if readOnly {
		q.Set("mode", "ro")
	} else {
		q.Add("_pragma", "journal_mode(WAL)")
		q.Add("_pragma", "synchronous(NORMAL)")
		q.Set("_txlock", "immediate")
	}
	return fmt.Sprintf("file:%s?%s", filepath.ToSlash(dbPath), q.Encode())
}

func (s *Store) Close() error {
	var errs []error
	if s.reader != nil {
		if err := s.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Store) CreateRun(ctx context.Context, rec store.RunRecord, initialEvents []engine.Event) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create run tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO runs (run_id, parent_run_id, fork_from_seq, workflow_name, workflow_hash, workflow_yaml, status, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.RunID, rec.ParentRunID, rec.ForkFromSeq, rec.WorkflowName, rec.WorkflowHash, rec.WorkflowYAML, string(rec.Status), rec.CreatedAtNs,
	)
	if err != nil {
		return fmt.Errorf("insert run %q: %w", rec.RunID, err)
	}

	if err := insertEventsTx(ctx, tx, initialEvents); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create run %q: %w", rec.RunID, err)
	}
	return nil
}

func (s *Store) Append(ctx context.Context, runID string, expectedLastSeq int64, events []engine.Event, newStatus engine.RunStatus) error {
	if s.readOnly {
		return store.ErrStoreReadOnly
	}
	if len(events) == 0 {
		return nil
	}

	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin append tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var forkFromSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT fork_from_seq FROM runs WHERE run_id = ?`, runID).Scan(&forkFromSeq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", store.ErrRunNotFound, runID)
		}
		return fmt.Errorf("select run %q: %w", runID, err)
	}

	var maxSeq sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(seq) FROM events WHERE run_id = ?`, runID).Scan(&maxSeq); err != nil {
		return fmt.Errorf("select max seq for %q: %w", runID, err)
	}
	currentLastSeq := forkFromSeq
	if maxSeq.Valid && maxSeq.Int64 > currentLastSeq {
		currentLastSeq = maxSeq.Int64
	}
	if currentLastSeq != expectedLastSeq {
		return fmt.Errorf("%w: run %q current=%d expected=%d", store.ErrSeqConflict, runID, currentLastSeq, expectedLastSeq)
	}

	if err := insertEventsTx(ctx, tx, events); err != nil {
		return err
	}

	if newStatus != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = ? WHERE run_id = ?`, string(newStatus), runID); err != nil {
			return fmt.Errorf("update run %q status: %w", runID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit append for %q: %w", runID, err)
	}
	return nil
}

func insertEventsTx(ctx context.Context, tx *sql.Tx, events []engine.Event) error {
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO events (run_id, seq, ts_ns, event_type, task_id, attempt, fencing_token, worker_id, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare insert event: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, ev := range events {
		payload := ""
		if len(ev.Payload) > 0 {
			payload = string(ev.Payload)
		}
		if _, err := stmt.ExecContext(ctx,
			ev.RunID, ev.Seq, ev.TimestampNs, string(ev.Type),
			ev.TaskID, ev.Attempt, ev.FencingToken, ev.WorkerID, payload,
		); err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return fmt.Errorf("%w: run=%s seq=%d", store.ErrSeqConflict, ev.RunID, ev.Seq)
			}
			return fmt.Errorf("insert event run=%s seq=%d: %w", ev.RunID, ev.Seq, err)
		}
	}
	return nil
}

func (s *Store) GetRun(ctx context.Context, runID string) (store.RunRecord, error) {
	var rec store.RunRecord
	var status string
	err := s.reader.QueryRowContext(ctx, `
		SELECT run_id, parent_run_id, fork_from_seq, workflow_name, workflow_hash, workflow_yaml, status, created_at_ns
		FROM runs WHERE run_id = ?`, runID,
	).Scan(&rec.RunID, &rec.ParentRunID, &rec.ForkFromSeq, &rec.WorkflowName, &rec.WorkflowHash, &rec.WorkflowYAML, &status, &rec.CreatedAtNs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.RunRecord{}, fmt.Errorf("%w: %s", store.ErrRunNotFound, runID)
		}
		return store.RunRecord{}, fmt.Errorf("get run %q: %w", runID, err)
	}
	rec.Status = engine.RunStatus(status)
	return rec, nil
}

func (s *Store) ListRuns(ctx context.Context, statuses ...engine.RunStatus) ([]store.RunRecord, error) {
	query := `SELECT run_id, parent_run_id, fork_from_seq, workflow_name, workflow_hash, workflow_yaml, status, created_at_ns FROM runs`
	var args []any
	if len(statuses) > 0 {
		placeholders := make([]string, len(statuses))
		for i, st := range statuses {
			placeholders[i] = "?"
			args = append(args, string(st))
		}
		query += ` WHERE status IN (` + strings.Join(placeholders, ",") + `)`
	}
	query += ` ORDER BY created_at_ns ASC, run_id ASC`

	rows, err := s.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []store.RunRecord
	for rows.Next() {
		var rec store.RunRecord
		var status string
		if err := rows.Scan(&rec.RunID, &rec.ParentRunID, &rec.ForkFromSeq, &rec.WorkflowName, &rec.WorkflowHash, &rec.WorkflowYAML, &status, &rec.CreatedAtNs); err != nil {
			return nil, fmt.Errorf("scan run row: %w", err)
		}
		rec.Status = engine.RunStatus(status)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate run rows: %w", err)
	}
	return out, nil
}

func (s *Store) LoadRunEvents(ctx context.Context, runID string) ([]engine.Event, error) {
	return s.loadLineageEvents(ctx, runID, 0, 0)
}

func (s *Store) loadLineageEvents(ctx context.Context, runID string, maxSeq int64, depth int) ([]engine.Event, error) {
	if depth > maxLineageDepth {
		return nil, fmt.Errorf("fork lineage depth exceeded %d for run %q", maxLineageDepth, runID)
	}
	rec, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	var combined []engine.Event
	if rec.ParentRunID != "" {
		parentCap := rec.ForkFromSeq
		if maxSeq > 0 && maxSeq < parentCap {
			parentCap = maxSeq
		}
		prefix, err := s.loadLineageEvents(ctx, rec.ParentRunID, parentCap, depth+1)
		if err != nil {
			return nil, err
		}
		combined = append(combined, prefix...)
	}

	query := `SELECT run_id, seq, ts_ns, event_type, task_id, attempt, fencing_token, worker_id, payload_json FROM events WHERE run_id = ?`
	args := []any{runID}
	if maxSeq > 0 {
		query += ` AND seq <= ?`
		args = append(args, maxSeq)
	}
	query += ` ORDER BY seq ASC`

	rows, err := s.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events for %q: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var ev engine.Event
		var evType, payload string
		if err := rows.Scan(&ev.RunID, &ev.Seq, &ev.TimestampNs, &evType, &ev.TaskID, &ev.Attempt, &ev.FencingToken, &ev.WorkerID, &payload); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		ev.Type = engine.EventType(evType)
		if payload != "" {
			ev.Payload = json.RawMessage(payload)
		}
		combined = append(combined, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event rows: %w", err)
	}
	return combined, nil
}
