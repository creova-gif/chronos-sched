package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestDLQRecordListAndRequeue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-dlq.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	baseTime := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC).UnixNano()

	entries := []store.DeadLetterEntry{
		{
			RunID:        "run-1",
			TaskID:       "fetch",
			WorkflowName: "etl",
			Attempt:      3,
			Reason:       "timeout after 30s",
			FailedAtNs:   baseTime,
		},
		{
			RunID:        "run-2",
			TaskID:       "transform",
			WorkflowName: "etl",
			Attempt:      2,
			Reason:       "non-retryable exit 1",
			FailedAtNs:   baseTime + int64(time.Second),
		},
	}

	for _, entry := range entries {
		if err := st.RecordDeadLetter(ctx, entry); err != nil {
			t.Fatalf("RecordDeadLetter(%s/%s): %v", entry.RunID, entry.TaskID, err)
		}
	}

	active, err := st.ListDeadLetters(ctx, false)
	if err != nil {
		t.Fatalf("ListDeadLetters(includeRequeued=false): %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active entries, got %d", len(active))
	}
	// Ordered newest-first.
	if active[0].RunID != "run-2" || active[1].RunID != "run-1" {
		t.Fatalf("unexpected ordering: got [%s, %s]", active[0].RunID, active[1].RunID)
	}
	if active[0].ID == 0 || active[1].ID == 0 {
		t.Fatalf("expected non-zero autoincrement IDs, got %d and %d", active[0].ID, active[1].ID)
	}

	// Mark the first (newest) entry as requeued.
	requeuedID := active[0].ID
	if err := st.MarkDeadLetterRequeued(ctx, requeuedID, "run-2-retry"); err != nil {
		t.Fatalf("MarkDeadLetterRequeued(%d): %v", requeuedID, err)
	}

	// Active listing should now only contain run-1.
	activeAfter, err := st.ListDeadLetters(ctx, false)
	if err != nil {
		t.Fatalf("ListDeadLetters(includeRequeued=false) after requeue: %v", err)
	}
	if len(activeAfter) != 1 || activeAfter[0].RunID != "run-1" {
		t.Fatalf("expected only run-1 active, got %+v", activeAfter)
	}

	// Full listing should include both with requeued metadata populated.
	allAfter, err := st.ListDeadLetters(ctx, true)
	if err != nil {
		t.Fatalf("ListDeadLetters(includeRequeued=true) after requeue: %v", err)
	}
	if len(allAfter) != 2 {
		t.Fatalf("expected 2 total entries, got %d", len(allAfter))
	}
	if !allAfter[0].Requeued || allAfter[0].RequeuedRunID != "run-2-retry" {
		t.Fatalf("expected run-2 entry marked requeued with run-2-retry, got %+v", allAfter[0])
	}
	if allAfter[1].Requeued || allAfter[1].RequeuedRunID != "" {
		t.Fatalf("expected run-1 entry not requeued, got %+v", allAfter[1])
	}

	// Requeueing a non-existent ID should return sql.ErrNoRows.
	err = st.MarkDeadLetterRequeued(ctx, 99999, "run-missing")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for missing ID, got %v", err)
	}
}

func TestDLQReadOnlyBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-dlq-ro.db")

	// Initialize database without creating any DLQ entries/table.
	rw, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("Close rw: %v", err)
	}

	roBefore, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly before DLQ writes: %v", err)
	}

	// Listing on a read-only DB without dead_letters table must succeed with empty slice.
	for _, includeRequeued := range []bool{false, true} {
		got, err := roBefore.ListDeadLetters(ctx, includeRequeued)
		if err != nil {
			t.Fatalf("ListDeadLetters(includeRequeued=%v) on missing table: %v", includeRequeued, err)
		}
		if len(got) != 0 {
			t.Fatalf("expected 0 entries on missing table, got %d", len(got))
		}
	}

	// Mutating calls must return store.ErrStoreReadOnly.
	if err := roBefore.RecordDeadLetter(ctx, store.DeadLetterEntry{
		RunID:      "run-ro",
		TaskID:     "step1",
		FailedAtNs: time.Now().UnixNano(),
	}); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly from RecordDeadLetter, got %v", err)
	}
	if err := roBefore.MarkDeadLetterRequeued(ctx, 1, "run-ro-retry"); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly from MarkDeadLetterRequeued, got %v", err)
	}
	if err := roBefore.Close(); err != nil {
		t.Fatalf("Close roBefore: %v", err)
	}

	// Re-open read-write, record a DLQ entry, and verify OpenReadOnly reads it.
	rw2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open rw2: %v", err)
	}
	failedAt := time.Date(2026, 11, 25, 13, 0, 0, 0, time.UTC).UnixNano()
	if err := rw2.RecordDeadLetter(ctx, store.DeadLetterEntry{
		RunID:        "run-10",
		TaskID:       "notify",
		WorkflowName: "alerts",
		Attempt:      4,
		Reason:       "webhook 503",
		FailedAtNs:   failedAt,
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}
	if err := rw2.Close(); err != nil {
		t.Fatalf("Close rw2: %v", err)
	}

	roAfter, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly after DLQ writes: %v", err)
	}
	defer func() { _ = roAfter.Close() }()

	got, err := roAfter.ListDeadLetters(ctx, false)
	if err != nil {
		t.Fatalf("ListDeadLetters after DLQ writes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	if got[0].RunID != "run-10" || got[0].TaskID != "notify" || got[0].WorkflowName != "alerts" ||
		got[0].Attempt != 4 || got[0].Reason != "webhook 503" || got[0].FailedAtNs != failedAt {
		t.Fatalf("unexpected entry: %+v", got[0])
	}
}

func TestDLQConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-dlq-race.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers * 2)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			if err := st.RecordDeadLetter(ctx, store.DeadLetterEntry{
				RunID:        fmt.Sprintf("run-%d", i),
				TaskID:       "task",
				WorkflowName: "concurrent",
				Attempt:      1,
				Reason:       "boom",
				FailedAtNs:   time.Now().UnixNano() + int64(i),
			}); err != nil {
				t.Errorf("RecordDeadLetter(%d): %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := st.ListDeadLetters(ctx, true); err != nil {
				t.Errorf("ListDeadLetters(%d): %v", i, err)
			}
		}()
	}
	wg.Wait()

	all, err := st.ListDeadLetters(ctx, true)
	if err != nil {
		t.Fatalf("final ListDeadLetters: %v", err)
	}
	if len(all) != workers {
		t.Fatalf("expected %d entries, got %d", workers, len(all))
	}
}
