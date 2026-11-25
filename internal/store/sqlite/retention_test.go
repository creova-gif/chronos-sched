package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func seedTestRun(t *testing.T, st *sqlite.Store, runID, parentRunID string, forkFromSeq int64, status engine.RunStatus, createdAtNs int64, evCount int) {
	t.Helper()
	ctx := context.Background()
	var evs []engine.Event
	startSeq := forkFromSeq + 1
	for i := 0; i < evCount; i++ {
		evType := engine.EventTaskCompleted
		if i == 0 && parentRunID == "" {
			evType = engine.EventRunStarted
		} else if i == 0 && parentRunID != "" {
			evType = engine.EventTimelineForked
		}
		evs = append(evs, engine.Event{
			RunID:       runID,
			Seq:         startSeq + int64(i),
			TimestampNs: createdAtNs + int64(i),
			Type:        evType,
		})
	}
	err := st.CreateRun(ctx, store.RunRecord{
		RunID:        runID,
		ParentRunID:  parentRunID,
		ForkFromSeq:  forkFromSeq,
		WorkflowName: "retention-test",
		WorkflowHash: "hash-1",
		WorkflowYAML: testYAML,
		Status:       status,
		CreatedAtNs:  createdAtNs,
	}, evs)
	if err != nil {
		t.Fatalf("CreateRun(%s): %v", runID, err)
	}
}

func TestPruneDryRunAndActualPrune(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	cutoff := base + int64(10*time.Hour)

	seedTestRun(t, st, "run_old_completed", "", 0, engine.RunStatusCompleted, base+int64(time.Hour), 3)
	seedTestRun(t, st, "run_old_failed", "", 0, engine.RunStatusFailed, base+int64(2*time.Hour), 2)
	seedTestRun(t, st, "run_old_cancelled", "", 0, engine.RunStatusCancelled, base+int64(3*time.Hour), 1)
	seedTestRun(t, st, "run_new_completed", "", 0, engine.RunStatusCompleted, base+int64(12*time.Hour), 2)

	// Dry-run should report candidates without mutating SQLite.
	dryRep, err := st.PruneTerminalRuns(ctx, store.PruneOptions{
		CutoffNs: cutoff,
		DryRun:   true,
		Vacuum:   true,
	})
	if err != nil {
		t.Fatalf("DryRun PruneTerminalRuns: %v", err)
	}
	wantCandidates := []string{"run_old_cancelled", "run_old_completed", "run_old_failed"}
	if !reflect.DeepEqual(dryRep.CandidateRunIDs, wantCandidates) {
		t.Fatalf("dry-run CandidateRunIDs = %v, want %v", dryRep.CandidateRunIDs, wantCandidates)
	}
	if dryRep.DeletedRuns != 0 || dryRep.DeletedEvents != 0 || dryRep.Vacuumed {
		t.Fatalf("dry-run mutated report: %+v", dryRep)
	}
	runs, err := st.ListRuns(ctx)
	if err != nil {
		t.Fatalf("ListRuns after dry-run: %v", err)
	}
	if len(runs) != 4 {
		t.Fatalf("expected 4 runs after dry-run, got %d", len(runs))
	}

	// Actual prune without vacuum.
	rep, err := st.PruneTerminalRuns(ctx, store.PruneOptions{
		CutoffNs: cutoff,
		DryRun:   false,
		Vacuum:   false,
	})
	if err != nil {
		t.Fatalf("PruneTerminalRuns: %v", err)
	}
	if !reflect.DeepEqual(rep.CandidateRunIDs, wantCandidates) {
		t.Fatalf("CandidateRunIDs = %v, want %v", rep.CandidateRunIDs, wantCandidates)
	}
	if rep.DeletedRuns != 3 || rep.DeletedEvents != 6 || rep.Vacuumed {
		t.Fatalf("unexpected prune report: %+v", rep)
	}

	runs, err = st.ListRuns(ctx)
	if err != nil {
		t.Fatalf("ListRuns after prune: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "run_new_completed" {
		t.Fatalf("remaining runs = %+v, want [run_new_completed]", runs)
	}
}

func TestPruneLineageProtectionAndMultiHopCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	cutoff := base + int64(10*time.Hour)

	// Multi-hop lineage: run_grandparent (old terminal) -> run_parent (old terminal) -> run_child (young terminal).
	seedTestRun(t, st, "run_grandparent", "", 0, engine.RunStatusCompleted, base+int64(time.Hour), 2)
	seedTestRun(t, st, "run_parent", "run_grandparent", 1, engine.RunStatusCompleted, base+int64(2*time.Hour), 2)
	seedTestRun(t, st, "run_child", "run_parent", 2, engine.RunStatusCompleted, base+int64(12*time.Hour), 2)

	// Separate old terminal parent with a still-running old child.
	seedTestRun(t, st, "run_parent_active_child", "", 0, engine.RunStatusCompleted, base+int64(time.Hour), 2)
	seedTestRun(t, st, "run_child_running", "run_parent_active_child", 1, engine.RunStatusRunning, base+int64(2*time.Hour), 1)

	rep, err := st.PruneTerminalRuns(ctx, store.PruneOptions{CutoffNs: cutoff})
	if err != nil {
		t.Fatalf("PruneTerminalRuns: %v", err)
	}
	if len(rep.CandidateRunIDs) != 0 || rep.DeletedRuns != 0 {
		t.Fatalf("expected 0 deletions while children protect ancestors, got %+v", rep)
	}
	wantSkipped := []string{"run_grandparent", "run_parent", "run_parent_active_child"}
	if !reflect.DeepEqual(rep.SkippedLineageParents, wantSkipped) {
		t.Fatalf("SkippedLineageParents = %v, want %v", rep.SkippedLineageParents, wantSkipped)
	}

	// Verify LoadRunEvents for run_child still resolves full multi-hop lineage.
	evs, err := st.LoadRunEvents(ctx, "run_child")
	if err != nil {
		t.Fatalf("LoadRunEvents(run_child): %v", err)
	}
	if len(evs) != 4 { // 1 from grandparent (seq<=1) + 1 from parent (seq<=2) + 2 from child
		t.Fatalf("LoadRunEvents(run_child) len = %d, want 4", len(evs))
	}

	// Now advance cutoff past run_child; grandparent -> parent -> child are all eligible and should be pruned together.
	laterCutoff := base + int64(20*time.Hour)
	rep2, err := st.PruneTerminalRuns(ctx, store.PruneOptions{CutoffNs: laterCutoff})
	if err != nil {
		t.Fatalf("second PruneTerminalRuns: %v", err)
	}
	wantCandidates2 := []string{"run_child", "run_grandparent", "run_parent"}
	if !reflect.DeepEqual(rep2.CandidateRunIDs, wantCandidates2) {
		t.Fatalf("second CandidateRunIDs = %v, want %v", rep2.CandidateRunIDs, wantCandidates2)
	}
	if rep2.DeletedRuns != 3 || rep2.DeletedEvents != 6 {
		t.Fatalf("second prune counts = %+v, want 3 runs / 6 events", rep2)
	}
	// run_parent_active_child is still protected by run_child_running.
	if !reflect.DeepEqual(rep2.SkippedLineageParents, []string{"run_parent_active_child"}) {
		t.Fatalf("second SkippedLineageParents = %v, want [run_parent_active_child]", rep2.SkippedLineageParents)
	}
}

func TestPruneNeverDeletesActiveRunsAndSupportsVacuum(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	cutoff := base + int64(10*time.Hour)

	seedTestRun(t, st, "run_old_running", "", 0, engine.RunStatusRunning, base+int64(time.Hour), 2)
	seedTestRun(t, st, "run_old_paused", "", 0, engine.RunStatusPaused, base+int64(2*time.Hour), 2)
	seedTestRun(t, st, "run_old_pending", "", 0, engine.RunStatusPending, base+int64(3*time.Hour), 1)
	seedTestRun(t, st, "run_old_done", "", 0, engine.RunStatusCompleted, base+int64(4*time.Hour), 3)

	rep, err := st.PruneTerminalRuns(ctx, store.PruneOptions{
		CutoffNs: cutoff,
		DryRun:   false,
		Vacuum:   true,
	})
	if err != nil {
		t.Fatalf("PruneTerminalRuns with Vacuum: %v", err)
	}
	if !rep.Vacuumed || rep.DeletedRuns != 1 || rep.DeletedEvents != 3 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	if !reflect.DeepEqual(rep.CandidateRunIDs, []string{"run_old_done"}) {
		t.Fatalf("CandidateRunIDs = %v, want [run_old_done]", rep.CandidateRunIDs)
	}

	// Remaining active runs must still be readable via LoadRunEvents after VACUUM.
	for _, id := range []string{"run_old_running", "run_old_paused", "run_old_pending"} {
		evs, err := st.LoadRunEvents(ctx, id)
		if err != nil {
			t.Fatalf("LoadRunEvents(%s) after VACUUM: %v", id, err)
		}
		if len(evs) == 0 {
			t.Fatalf("expected events for %s after VACUUM", id)
		}
	}

	// Read-only store must reject PruneTerminalRuns.
	ro, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer func() { _ = ro.Close() }()
	_, err = ro.PruneTerminalRuns(ctx, store.PruneOptions{CutoffNs: cutoff})
	if !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly, got %v", err)
	}
}
