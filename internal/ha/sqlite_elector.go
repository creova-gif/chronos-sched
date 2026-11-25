package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DefaultLockName is used when no explicit lock name is provided.
const DefaultLockName = "coordinator"

const electorSchemaSQL = `
CREATE TABLE IF NOT EXISTS coordinator_leader (
	lock_name     TEXT PRIMARY KEY,
	holder_id     TEXT NOT NULL,
	epoch         INTEGER NOT NULL,
	expires_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS coordinator_leader_epochs (
	lock_name  TEXT PRIMARY KEY,
	last_epoch INTEGER NOT NULL
);
`

// SQLiteElector implements LeaderElector on top of a SQLite WAL database using
// immediate transactions and a single-connection writer pool (ADR-001, ADR-004).
type SQLiteElector struct {
	db       *sql.DB
	lockName string

	nowMu sync.RWMutex
	nowFn func() time.Time
}

// OpenSQLiteElector opens (or creates) the SQLite WAL database at dbPath and
// initializes the coordinator leader lease tables.
func OpenSQLiteElector(dbPath string, lockName string) (*SQLiteElector, error) {
	if lockName == "" {
		lockName = DefaultLockName
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite parent dir: %w", err)
	}

	dsn := buildElectorDSN(dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite elector: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite elector: %w", err)
	}
	if _, err := db.ExecContext(ctx, electorSchemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init sqlite elector schema: %w", err)
	}

	return &SQLiteElector{
		db:       db,
		lockName: lockName,
		nowFn:    time.Now,
	}, nil
}

func buildElectorDSN(dbPath string) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(2000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Set("_txlock", "immediate")
	return fmt.Sprintf("file:%s?%s", filepath.ToSlash(dbPath), q.Encode())
}

// SetNowFunc overrides the clock function used for lease expiry checks.
// Passing nil restores time.Now. Intended for deterministic testing.
func (e *SQLiteElector) SetNowFunc(fn func() time.Time) {
	e.nowMu.Lock()
	defer e.nowMu.Unlock()
	if fn == nil {
		e.nowFn = time.Now
		return
	}
	e.nowFn = fn
}

func (e *SQLiteElector) now() time.Time {
	e.nowMu.RLock()
	fn := e.nowFn
	e.nowMu.RUnlock()
	return fn()
}

// Close closes the underlying SQLite connection pool.
func (e *SQLiteElector) Close() error {
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}

// TryAcquire attempts to acquire the leadership lease in an immediate
// transaction. If no row exists, it inserts a new lease with a monotonically
// increasing epoch (1 for a fresh lock, or last_epoch+1 after a previous
// release). If a row exists and is expired or already held by candidateID, it
// atomically takes over with epoch = epoch + 1. Otherwise it returns the
// current active lease and acquired=false.
func (e *SQLiteElector) TryAcquire(ctx context.Context, candidateID string, ttl time.Duration) (LeaderLease, bool, error) {
	if candidateID == "" {
		return LeaderLease{}, false, errors.New("ha: candidateID must not be empty")
	}
	if ttl <= 0 {
		return LeaderLease{}, false, errors.New("ha: ttl must be positive")
	}

	nowNs := e.now().UnixNano()
	expiresAtNs := nowNs + ttl.Nanoseconds()

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return LeaderLease{}, false, fmt.Errorf("begin try acquire tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var curHolder string
	var curEpoch, curExpires int64
	err = tx.QueryRowContext(ctx,
		`SELECT holder_id, epoch, expires_at_ns FROM coordinator_leader WHERE lock_name = ?`,
		e.lockName,
	).Scan(&curHolder, &curEpoch, &curExpires)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		var lastEpoch int64
		epochErr := tx.QueryRowContext(ctx,
			`SELECT last_epoch FROM coordinator_leader_epochs WHERE lock_name = ?`,
			e.lockName,
		).Scan(&lastEpoch)
		if epochErr != nil && !errors.Is(epochErr, sql.ErrNoRows) {
			return LeaderLease{}, false, fmt.Errorf("select last epoch for %q: %w", e.lockName, epochErr)
		}

		nextEpoch := lastEpoch + 1
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO coordinator_leader (lock_name, holder_id, epoch, expires_at_ns) VALUES (?, ?, ?, ?)`,
			e.lockName, candidateID, nextEpoch, expiresAtNs,
		); err != nil {
			return LeaderLease{}, false, fmt.Errorf("insert leader lease %q: %w", e.lockName, err)
		}
		if err := upsertLastEpochTx(ctx, tx, e.lockName, nextEpoch); err != nil {
			return LeaderLease{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return LeaderLease{}, false, fmt.Errorf("commit try acquire %q: %w", e.lockName, err)
		}
		return LeaderLease{
			LockName:    e.lockName,
			HolderID:    candidateID,
			Epoch:       nextEpoch,
			ExpiresAtNs: expiresAtNs,
		}, true, nil

	case err != nil:
		return LeaderLease{}, false, fmt.Errorf("select leader lease %q: %w", e.lockName, err)
	}

	if curExpires > nowNs && curHolder != candidateID {
		return LeaderLease{
			LockName:    e.lockName,
			HolderID:    curHolder,
			Epoch:       curEpoch,
			ExpiresAtNs: curExpires,
		}, false, nil
	}

	nextEpoch := curEpoch + 1
	if _, err := tx.ExecContext(ctx,
		`UPDATE coordinator_leader SET holder_id = ?, epoch = ?, expires_at_ns = ? WHERE lock_name = ?`,
		candidateID, nextEpoch, expiresAtNs, e.lockName,
	); err != nil {
		return LeaderLease{}, false, fmt.Errorf("takeover leader lease %q: %w", e.lockName, err)
	}
	if err := upsertLastEpochTx(ctx, tx, e.lockName, nextEpoch); err != nil {
		return LeaderLease{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return LeaderLease{}, false, fmt.Errorf("commit takeover %q: %w", e.lockName, err)
	}
	return LeaderLease{
		LockName:    e.lockName,
		HolderID:    candidateID,
		Epoch:       nextEpoch,
		ExpiresAtNs: expiresAtNs,
	}, true, nil
}

func upsertLastEpochTx(ctx context.Context, tx *sql.Tx, lockName string, epoch int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_leader_epochs (lock_name, last_epoch)
		VALUES (?, ?)
		ON CONFLICT(lock_name) DO UPDATE SET last_epoch = MAX(last_epoch, excluded.last_epoch)`,
		lockName, epoch,
	)
	if err != nil {
		return fmt.Errorf("upsert last epoch %q: %w", lockName, err)
	}
	return nil
}

// Renew extends expires_at_ns only if the caller still owns the unexpired lease
// at the exact matching epoch. Returns ErrLeaseLost if 0 rows were updated.
func (e *SQLiteElector) Renew(ctx context.Context, lease LeaderLease, ttl time.Duration) (LeaderLease, error) {
	if ttl <= 0 {
		return LeaderLease{}, errors.New("ha: ttl must be positive")
	}

	nowNs := e.now().UnixNano()
	expiresAtNs := nowNs + ttl.Nanoseconds()

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("begin renew tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`UPDATE coordinator_leader
		 SET expires_at_ns = ?
		 WHERE lock_name = ? AND holder_id = ? AND epoch = ? AND expires_at_ns > ?`,
		expiresAtNs, e.lockName, lease.HolderID, lease.Epoch, nowNs,
	)
	if err != nil {
		return LeaderLease{}, fmt.Errorf("renew leader lease %q: %w", e.lockName, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return LeaderLease{}, fmt.Errorf("renew rows affected %q: %w", e.lockName, err)
	}
	if affected == 0 {
		return LeaderLease{}, ErrLeaseLost
	}

	if err := tx.Commit(); err != nil {
		return LeaderLease{}, fmt.Errorf("commit renew %q: %w", e.lockName, err)
	}

	return LeaderLease{
		LockName:    e.lockName,
		HolderID:    lease.HolderID,
		Epoch:       lease.Epoch,
		ExpiresAtNs: expiresAtNs,
	}, nil
}

// Release deletes the lease row only if it still matches lock_name, holder_id,
// and epoch so a stale leader cannot evict a newer leader.
func (e *SQLiteElector) Release(ctx context.Context, lease LeaderLease) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin release tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM coordinator_leader WHERE lock_name = ? AND holder_id = ? AND epoch = ?`,
		e.lockName, lease.HolderID, lease.Epoch,
	); err != nil {
		return fmt.Errorf("release leader lease %q: %w", e.lockName, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit release %q: %w", e.lockName, err)
	}
	return nil
}

// CurrentLeader returns the currently active, unexpired leader lease or
// ErrNoLeader if none exists.
func (e *SQLiteElector) CurrentLeader(ctx context.Context) (LeaderLease, error) {
	nowNs := e.now().UnixNano()

	var lease LeaderLease
	lease.LockName = e.lockName
	err := e.db.QueryRowContext(ctx,
		`SELECT holder_id, epoch, expires_at_ns FROM coordinator_leader WHERE lock_name = ?`,
		e.lockName,
	).Scan(&lease.HolderID, &lease.Epoch, &lease.ExpiresAtNs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return LeaderLease{}, ErrNoLeader
		}
		return LeaderLease{}, fmt.Errorf("query current leader %q: %w", e.lockName, err)
	}
	if lease.ExpiresAtNs <= nowNs {
		return LeaderLease{}, ErrNoLeader
	}
	return lease, nil
}
