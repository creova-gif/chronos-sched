package schedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// MisfirePolicy controls how the runner handles missed schedule slots after
// coordinator downtime or delayed ticks.
type MisfirePolicy string

const (
	// MisfireSkip fires the current due slot once and advances next_run_at_ns
	// strictly past now, skipping any intermediate missed slots.
	MisfireSkip MisfirePolicy = "skip"
	// MisfireCatchUp fires each missed slot in chronological order until
	// next_run_at_ns is strictly past now.
	MisfireCatchUp MisfirePolicy = "catch_up"
)

// ErrScheduleNotFound is returned when a schedule ID does not exist.
var ErrScheduleNotFound = errors.New("schedule not found")

// ScheduleRecord is the persisted definition and cursor state of a recurring schedule.
type ScheduleRecord struct {
	ID            string
	CronExpr      string
	WorkflowYAML  string
	MisfirePolicy MisfirePolicy
	NextRunAtNs   int64
	LastRunAtNs   int64
	Enabled       bool
	CreatedAtNs   int64
}

const scheduleSchemaSQL = `
CREATE TABLE IF NOT EXISTS schedules (
	id             TEXT PRIMARY KEY,
	cron_expr      TEXT NOT NULL,
	workflow_yaml  TEXT NOT NULL,
	misfire_policy TEXT NOT NULL DEFAULT 'skip',
	next_run_at_ns INTEGER NOT NULL,
	last_run_at_ns INTEGER NOT NULL DEFAULT 0,
	enabled        INTEGER NOT NULL DEFAULT 1,
	created_at_ns  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_run_at_ns);
`

// SQLiteStore persists recurring schedules in SQLite using WAL mode and
// single-connection serialized immediate transactions (matching ADR-001).
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLiteStore opens or creates the SQLite schedule store at dbPath.
func OpenSQLiteStore(dbPath string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite parent dir: %w", err)
	}

	dsn := buildScheduleDSN(dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open schedule sqlite db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping schedule sqlite db: %w", err)
	}
	if _, err := db.ExecContext(ctx, scheduleSchemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schedule sqlite schema: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func buildScheduleDSN(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(2000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Set("_txlock", "immediate")
	return fmt.Sprintf("file:%s?%s", filepath.ToSlash(dbPath), q.Encode())
}

// Close closes the underlying SQLite connection pool.
func (s *SQLiteStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Upsert inserts a new schedule or updates an existing schedule by ID.
func (s *SQLiteStore) Upsert(ctx context.Context, rec ScheduleRecord) error {
	if rec.ID == "" {
		return errors.New("schedule ID must not be empty")
	}
	if rec.CronExpr == "" {
		return errors.New("schedule cron expression must not be empty")
	}
	policy := rec.MisfirePolicy
	if policy == "" {
		policy = MisfireSkip
	}
	if policy != MisfireSkip && policy != MisfireCatchUp {
		return fmt.Errorf("invalid misfire policy %q", policy)
	}

	enabledInt := 0
	if rec.Enabled {
		enabledInt = 1
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO schedules (id, cron_expr, workflow_yaml, misfire_policy, next_run_at_ns, last_run_at_ns, enabled, created_at_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			cron_expr      = excluded.cron_expr,
			workflow_yaml  = excluded.workflow_yaml,
			misfire_policy = excluded.misfire_policy,
			next_run_at_ns = excluded.next_run_at_ns,
			last_run_at_ns = excluded.last_run_at_ns,
			enabled        = excluded.enabled`,
		rec.ID, rec.CronExpr, rec.WorkflowYAML, string(policy), rec.NextRunAtNs, rec.LastRunAtNs, enabledInt, rec.CreatedAtNs,
	)
	if err != nil {
		return fmt.Errorf("upsert schedule %q: %w", rec.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert schedule %q: %w", rec.ID, err)
	}
	return nil
}

// Get retrieves a schedule by ID.
func (s *SQLiteStore) Get(ctx context.Context, id string) (ScheduleRecord, error) {
	var rec ScheduleRecord
	var policy string
	var enabledInt int

	err := s.db.QueryRowContext(ctx, `
		SELECT id, cron_expr, workflow_yaml, misfire_policy, next_run_at_ns, last_run_at_ns, enabled, created_at_ns
		FROM schedules WHERE id = ?`, id,
	).Scan(&rec.ID, &rec.CronExpr, &rec.WorkflowYAML, &policy, &rec.NextRunAtNs, &rec.LastRunAtNs, &enabledInt, &rec.CreatedAtNs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ScheduleRecord{}, fmt.Errorf("%w: %s", ErrScheduleNotFound, id)
		}
		return ScheduleRecord{}, fmt.Errorf("get schedule %q: %w", id, err)
	}

	rec.MisfirePolicy = MisfirePolicy(policy)
	rec.Enabled = enabledInt != 0
	return rec, nil
}

// List returns all schedules ordered by creation timestamp and ID.
func (s *SQLiteStore) List(ctx context.Context) ([]ScheduleRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, cron_expr, workflow_yaml, misfire_policy, next_run_at_ns, last_run_at_ns, enabled, created_at_ns
		FROM schedules
		ORDER BY created_at_ns ASC, id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanScheduleRows(rows)
}

// Delete removes a schedule by ID.
func (s *SQLiteStore) Delete(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `DELETE FROM schedules WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete schedule %q: %w", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected deleting schedule %q: %w", id, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %s", ErrScheduleNotFound, id)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete schedule %q: %w", id, err)
	}
	return nil
}

// ListDue returns all enabled schedules whose next_run_at_ns is <= nowNs,
// ordered by next_run_at_ns ASC, id ASC.
func (s *SQLiteStore) ListDue(ctx context.Context, nowNs int64) ([]ScheduleRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, cron_expr, workflow_yaml, misfire_policy, next_run_at_ns, last_run_at_ns, enabled, created_at_ns
		FROM schedules
		WHERE enabled = 1 AND next_run_at_ns <= ?
		ORDER BY next_run_at_ns ASC, id ASC`, nowNs)
	if err != nil {
		return nil, fmt.Errorf("list due schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanScheduleRows(rows)
}

// Advance performs an atomic compare-and-swap on next_run_at_ns for an enabled
// schedule. It returns (true, nil) if the row matched expectedNextRunNs and was
// updated, or (false, nil) if another tick/coordinator already advanced it or
// the schedule was disabled/deleted.
func (s *SQLiteStore) Advance(ctx context.Context, id string, expectedNextRunNs, newLastRunNs, newNextRunNs int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin advance schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		UPDATE schedules
		SET last_run_at_ns = ?, next_run_at_ns = ?
		WHERE id = ? AND enabled = 1 AND next_run_at_ns = ?`,
		newLastRunNs, newNextRunNs, id, expectedNextRunNs,
	)
	if err != nil {
		return false, fmt.Errorf("advance schedule %q: %w", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected advancing schedule %q: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit advance schedule %q: %w", id, err)
	}
	return affected == 1, nil
}

func scanScheduleRows(rows *sql.Rows) ([]ScheduleRecord, error) {
	var out []ScheduleRecord
	for rows.Next() {
		var rec ScheduleRecord
		var policy string
		var enabledInt int
		if err := rows.Scan(
			&rec.ID, &rec.CronExpr, &rec.WorkflowYAML, &policy,
			&rec.NextRunAtNs, &rec.LastRunAtNs, &enabledInt, &rec.CreatedAtNs,
		); err != nil {
			return nil, fmt.Errorf("scan schedule row: %w", err)
		}
		rec.MisfirePolicy = MisfirePolicy(policy)
		rec.Enabled = enabledInt != 0
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schedule rows: %w", err)
	}
	return out, nil
}
