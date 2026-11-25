package schedule

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseSpecAndNext(t *testing.T) {
	t.Run("every_15_minutes", func(t *testing.T) {
		spec, err := ParseSpec("*/15 * * * *")
		if err != nil {
			t.Fatalf("ParseSpec failed: %v", err)
		}

		after := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
		got := spec.Next(after)
		want := time.Date(2026, 6, 1, 10, 15, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("Next(%v) = %v, want %v", after, got, want)
		}

		after2 := time.Date(2026, 6, 1, 10, 46, 12, 0, time.UTC)
		got2 := spec.Next(after2)
		want2 := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
		if !got2.Equal(want2) {
			t.Fatalf("Next(%v) = %v, want %v", after2, got2, want2)
		}
	})

	t.Run("weekdays_0930", func(t *testing.T) {
		spec, err := ParseSpec("30 9 * * 1-5")
		if err != nil {
			t.Fatalf("ParseSpec failed: %v", err)
		}

		// Friday 2026-06-05 09:30:00 UTC -> next is Monday 2026-06-08 09:30:00 UTC.
		fri := time.Date(2026, 6, 5, 9, 30, 0, 0, time.UTC)
		got := spec.Next(fri)
		want := time.Date(2026, 6, 8, 9, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("Next(%v) = %v, want %v", fri, got, want)
		}
	})

	t.Run("comma_lists_and_sunday_7", func(t *testing.T) {
		spec, err := ParseSpec("0,30 8,20 * * 7")
		if err != nil {
			t.Fatalf("ParseSpec failed: %v", err)
		}
		// Saturday 2026-06-06 23:00 UTC -> Sunday 2026-06-07 08:00 UTC.
		sat := time.Date(2026, 6, 6, 23, 0, 0, 0, time.UTC)
		got := spec.Next(sat)
		want := time.Date(2026, 6, 7, 8, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("Next(%v) = %v, want %v", sat, got, want)
		}
	})

	t.Run("shorthands", func(t *testing.T) {
		base := time.Date(2026, 6, 1, 14, 22, 10, 0, time.UTC)

		cases := []struct {
			expr string
			want time.Time
		}{
			{"@every 30s", base.Add(30 * time.Second)},
			{"@hourly", time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC)},
			{"@daily", time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)},
			{"@midnight", time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)},
		}

		for _, tc := range cases {
			spec, err := ParseSpec(tc.expr)
			if err != nil {
				t.Fatalf("ParseSpec(%q) failed: %v", tc.expr, err)
			}
			got := spec.Next(base)
			if !got.Equal(tc.want) {
				t.Errorf("ParseSpec(%q).Next(%v) = %v, want %v", tc.expr, base, got, tc.want)
			}
		}
	})

	t.Run("invalid_expressions", func(t *testing.T) {
		invalid := []string{
			"",
			"   ",
			"* * * *",
			"* * * * * *",
			"60 * * * *",
			"* 24 * * *",
			"* * 0 * *",
			"* * 32 * *",
			"* * * 13 *",
			"* * * * 8",
			"*/0 * * * *",
			"*/-5 * * * *",
			"10-5 * * * *",
			"1,,2 * * * *",
			"5/2 * * * *",
			"@every",
			"@every 500ms",
			"@every 0s",
			"@every -10s",
			"@every invalid",
			"@weekly",
		}
		for _, expr := range invalid {
			if _, err := ParseSpec(expr); err == nil {
				t.Errorf("ParseSpec(%q) unexpectedly succeeded", expr)
			}
		}
	})
}

func TestSQLiteStoreCRUDAndAtomicAdvance(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "schedules.db")

	st, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLiteStore: %v", err)
	}
	defer func() { _ = st.Close() }()

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	rec := ScheduleRecord{
		ID:            "nightly-etl",
		CronExpr:      "@daily",
		WorkflowYAML:  "name: etl\ntasks: []\n",
		MisfirePolicy: MisfireSkip,
		NextRunAtNs:   t0.UnixNano(),
		LastRunAtNs:   0,
		Enabled:       true,
		CreatedAtNs:   t0.Add(-time.Hour).UnixNano(),
	}

	if err := st.Upsert(ctx, rec); err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}

	got, err := st.Get(ctx, "nightly-etl")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != rec {
		t.Fatalf("Get mismatch:\ngot:  %+v\nwant: %+v", got, rec)
	}

	// Update via Upsert.
	rec.CronExpr = "@hourly"
	if err := st.Upsert(ctx, rec); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].CronExpr != "@hourly" {
		t.Fatalf("List unexpected: %+v", list)
	}

	due, err := st.ListDue(ctx, t0.UnixNano())
	if err != nil {
		t.Fatalf("ListDue: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("ListDue len = %d, want 1", len(due))
	}

	// Concurrent atomic CAS Advance: exactly 1 of 16 goroutines must win.
	const contenders = 16
	var wins atomic.Int32
	var wg sync.WaitGroup
	nextSlot := t0.Add(time.Hour).UnixNano()

	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, advErr := st.Advance(ctx, "nightly-etl", t0.UnixNano(), t0.UnixNano(), nextSlot)
			if advErr != nil {
				t.Errorf("Advance error: %v", advErr)
				return
			}
			if ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	if wins.Load() != 1 {
		t.Fatalf("concurrent Advance wins = %d, want exactly 1", wins.Load())
	}

	// Stale expectedNextRunNs must return (false, nil).
	ok, err := st.Advance(ctx, "nightly-etl", t0.UnixNano(), t0.UnixNano(), nextSlot)
	if err != nil {
		t.Fatalf("stale Advance error: %v", err)
	}
	if ok {
		t.Fatalf("stale Advance unexpectedly succeeded")
	}

	updated, err := st.Get(ctx, "nightly-etl")
	if err != nil {
		t.Fatalf("Get after Advance: %v", err)
	}
	if updated.NextRunAtNs != nextSlot || updated.LastRunAtNs != t0.UnixNano() {
		t.Fatalf("post-Advance cursor mismatch: %+v", updated)
	}

	if err := st.Delete(ctx, "nightly-etl"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get(ctx, "nightly-etl"); !errors.Is(err, ErrScheduleNotFound) {
		t.Fatalf("Get after Delete err = %v, want ErrScheduleNotFound", err)
	}
}

func TestRunnerTickMisfirePoliciesAndRestartDurability(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "runner.db")

	st, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLiteStore: %v", err)
	}

	t0 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)

	// Schedule A: MisfireSkip, @every 30s, due at 10:00:00.
	if err := st.Upsert(ctx, ScheduleRecord{
		ID:            "skip-job",
		CronExpr:      "@every 30s",
		WorkflowYAML:  "name: skip\n",
		MisfirePolicy: MisfireSkip,
		NextRunAtNs:   t0.UnixNano(),
		Enabled:       true,
		CreatedAtNs:   t0.Add(-time.Minute).UnixNano(),
	}); err != nil {
		t.Fatalf("Upsert skip-job: %v", err)
	}

	// Schedule B: MisfireCatchUp, @every 30s, due at 10:00:00.
	if err := st.Upsert(ctx, ScheduleRecord{
		ID:            "catchup-job",
		CronExpr:      "@every 30s",
		WorkflowYAML:  "name: catchup\n",
		MisfirePolicy: MisfireCatchUp,
		NextRunAtNs:   t0.UnixNano(),
		Enabled:       true,
		CreatedAtNs:   t0.Add(-time.Minute).UnixNano(),
	}); err != nil {
		t.Fatalf("Upsert catchup-job: %v", err)
	}

	// Simulate downtime: current clock is 10:01:15 (missed 10:00:00, 10:00:30, 10:01:00).
	now := t0.Add(75 * time.Second)
	var mu sync.Mutex
	var submitted []string

	runner := NewRunner(st, func(_ context.Context, runID string, _ string) error {
		mu.Lock()
		submitted = append(submitted, runID)
		mu.Unlock()
		return nil
	}, func() time.Time { return now })

	fired, err := runner.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	// skip-job fires 1 time; catchup-job fires 3 times (10:00:00, 10:00:30, 10:01:00).
	if fired != 4 {
		t.Fatalf("Tick 1 fired = %d, want 4 (submitted=%v)", fired, submitted)
	}

	wantIDs := []string{
		DeterministicRunID("catchup-job", t0),
		DeterministicRunID("catchup-job", t0.Add(30*time.Second)),
		DeterministicRunID("catchup-job", t0.Add(60*time.Second)),
		DeterministicRunID("skip-job", t0),
	}
	if len(submitted) != len(wantIDs) {
		t.Fatalf("submitted len = %d, want %d: %v", len(submitted), len(wantIDs), submitted)
	}
	gotSet := make(map[string]bool, len(submitted))
	for _, id := range submitted {
		gotSet[id] = true
	}
	for _, want := range wantIDs {
		if !gotSet[want] {
			t.Errorf("missing expected runID %q in %v", want, submitted)
		}
	}

	// Verify cursor advancement.
	skipRec, err := st.Get(ctx, "skip-job")
	if err != nil {
		t.Fatalf("Get skip-job: %v", err)
	}
	if wantNext := now.Add(30 * time.Second).UnixNano(); skipRec.NextRunAtNs != wantNext {
		t.Fatalf("skip-job NextRunAtNs = %d, want %d", skipRec.NextRunAtNs, wantNext)
	}

	catchupRec, err := st.Get(ctx, "catchup-job")
	if err != nil {
		t.Fatalf("Get catchup-job: %v", err)
	}
	if wantNext := t0.Add(90 * time.Second).UnixNano(); catchupRec.NextRunAtNs != wantNext {
		t.Fatalf("catchup-job NextRunAtNs = %d, want %d", catchupRec.NextRunAtNs, wantNext)
	}

	// Immediate second tick at the same clock must fire nothing.
	fired2, err := runner.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	if fired2 != 0 {
		t.Fatalf("Tick 2 fired = %d, want 0", fired2)
	}

	// Simulate coordinator restart: close and reopen SQLiteStore from disk.
	if err := st.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}
	st2, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLiteStore: %v", err)
	}
	defer func() { _ = st2.Close() }()

	var restartedRuns []string
	runner2 := NewRunner(st2, func(_ context.Context, runID string, _ string) error {
		restartedRuns = append(restartedRuns, runID)
		return nil
	}, func() time.Time { return now })

	// Tick right after restart at same `now` must NOT double-fire anything.
	firedAfterRestart, err := runner2.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick after restart: %v", err)
	}
	if firedAfterRestart != 0 {
		t.Fatalf("Tick after restart fired = %d, want 0 (runs=%v)", firedAfterRestart, restartedRuns)
	}

	// Advance clock to 10:01:30 -> catchup-job is now due (10:01:30), skip-job is due at 10:01:45.
	now = t0.Add(90 * time.Second)
	firedNext, err := runner2.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick at 10:01:30: %v", err)
	}
	if firedNext != 1 || len(restartedRuns) != 1 || restartedRuns[0] != DeterministicRunID("catchup-job", t0.Add(90*time.Second)) {
		t.Fatalf("Tick at 10:01:30 unexpected: fired=%d runs=%v", firedNext, restartedRuns)
	}
}
