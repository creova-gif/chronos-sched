package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func buildCompletedTimeTravelRun(t *testing.T) (*engine.DAG, []engine.Event) {
	t.Helper()

	yamlSpec := `
name: timetravel-pipeline
tasks:
  - id: ingest
    handler: sim
    checkpoint_name: after_ingest
  - id: sidecar
    handler: sim
    checkpoint_name: after_sidecar
  - id: transform
    handler: sim
    depends_on: [ingest]
    checkpoint_name: after_transform
`
	dag, err := engine.ParseDAG([]byte(yamlSpec))
	if err != nil {
		t.Fatalf("ParseDAG failed: %v", err)
	}

	now := time.Unix(1700000000, 0)
	var journal []engine.Event
	state := engine.NewRunState(dag, "run-tt-1")

	applyStep := func(cmd engine.Command) {
		t.Helper()
		evs, _, err := engine.Step(dag, state, cmd, now)
		if err != nil {
			t.Fatalf("Step(%T) failed: %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply(%s) failed: %v", ev.Type, err)
			}
			journal = append(journal, ev)
		}
		now = now.Add(time.Second)
	}

	// 1. Start run.
	applyStep(engine.CmdStartRun{RunID: "run-tt-1"})
	// 2. Lease ingest on w1 and sidecar on w2.
	applyStep(engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second})
	applyStep(engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 5 * time.Second})
	// 3. Complete ingest (saves checkpoint 1: "after_ingest") while sidecar is still LEASED and transform is PENDING.
	applyStep(engine.CmdCompleteTask{
		TaskID:       "ingest",
		WorkerID:     "w1",
		FencingToken: state.Tasks["ingest"].FencingToken,
		Output:       map[string]any{"rows": float64(42)},
	})
	// 4. Complete sidecar (saves checkpoint 2: "after_sidecar").
	applyStep(engine.CmdCompleteTask{
		TaskID:       "sidecar",
		WorkerID:     "w2",
		FencingToken: state.Tasks["sidecar"].FencingToken,
		Output:       map[string]any{"ok": true},
	})
	// 5. Lease and complete transform (saves checkpoint 3 + RUN_COMPLETED).
	applyStep(engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second})
	applyStep(engine.CmdCompleteTask{
		TaskID:       "transform",
		WorkerID:     "w1",
		FencingToken: state.Tasks["transform"].FencingToken,
		Output:       map[string]any{"done": true},
	})

	if state.Status != engine.RunStatusCompleted {
		t.Fatalf("expected final run status COMPLETED, got %s", state.Status)
	}
	return dag, journal
}

func TestFilterEvents(t *testing.T) {
	t.Parallel()

	_, journal := buildCompletedTimeTravelRun(t)

	t.Run("filter by TaskID", func(t *testing.T) {
		t.Parallel()
		got := engine.FilterEvents(journal, engine.EventFilter{TaskID: "ingest"})
		if len(got) != 3 {
			t.Fatalf("expected 3 events for ingest (leased, completed, checkpoint_saved), got %d", len(got))
		}
		for _, ev := range got {
			if ev.TaskID != "ingest" {
				t.Fatalf("unexpected task_id %q", ev.TaskID)
			}
		}
	})

	t.Run("filter by WorkerID", func(t *testing.T) {
		t.Parallel()
		got := engine.FilterEvents(journal, engine.EventFilter{WorkerID: "w2"})
		if len(got) != 2 {
			t.Fatalf("expected 2 events for worker w2, got %d", len(got))
		}
		for _, ev := range got {
			if ev.WorkerID != "w2" {
				t.Fatalf("unexpected worker_id %q", ev.WorkerID)
			}
		}
	})

	t.Run("filter by EventType", func(t *testing.T) {
		t.Parallel()
		got := engine.FilterEvents(journal, engine.EventFilter{EventType: engine.EventCheckpointSaved})
		if len(got) != 3 {
			t.Fatalf("expected 3 CHECKPOINT_SAVED events, got %d", len(got))
		}
		for _, ev := range got {
			if ev.Type != engine.EventCheckpointSaved {
				t.Fatalf("unexpected event_type %s", ev.Type)
			}
		}
	})

	t.Run("filter by MinSeq and MaxSeq range", func(t *testing.T) {
		t.Parallel()
		got := engine.FilterEvents(journal, engine.EventFilter{MinSeq: 2, MaxSeq: 5})
		if len(got) != 4 {
			t.Fatalf("expected 4 events in seq [2,5], got %d", len(got))
		}
		for i, ev := range got {
			wantSeq := int64(i + 2)
			if ev.Seq != wantSeq {
				t.Fatalf("expected seq %d at index %d, got %d", wantSeq, i, ev.Seq)
			}
		}
	})

	t.Run("chronological Seq order preserved on unsorted input", func(t *testing.T) {
		t.Parallel()
		reversed := make([]engine.Event, len(journal))
		for i := range journal {
			reversed[i] = journal[len(journal)-1-i]
		}
		got := engine.FilterEvents(reversed, engine.EventFilter{WorkerID: "w1"})
		if len(got) == 0 {
			t.Fatal("expected non-empty filtered events")
		}
		for i := 1; i < len(got); i++ {
			if got[i].Seq <= got[i-1].Seq {
				t.Fatalf("expected strictly increasing Seq, got %d after %d", got[i].Seq, got[i-1].Seq)
			}
		}
	})
}

func TestStateAtCheckpoint(t *testing.T) {
	t.Parallel()

	dag, journal := buildCompletedTimeTravelRun(t)

	t.Run("reconstructs intermediate historical state by checkpoint name", func(t *testing.T) {
		t.Parallel()
		hist, cp, err := engine.StateAtCheckpoint(dag, journal, "after_ingest")
		if err != nil {
			t.Fatalf("StateAtCheckpoint(after_ingest) failed: %v", err)
		}
		if cp.Number != 1 || cp.Name != "after_ingest" || cp.TaskID != "ingest" {
			t.Fatalf("unexpected checkpoint info: %+v", cp)
		}
		if hist.Status != engine.RunStatusRunning {
			t.Fatalf("expected historical run status RUNNING, got %s", hist.Status)
		}
		if hist.LastSeq != cp.Seq {
			t.Fatalf("expected LastSeq=%d, got %d", cp.Seq, hist.LastSeq)
		}
		if hist.Tasks["ingest"].Status != engine.TaskStatusCompleted {
			t.Fatalf("expected ingest COMPLETED at checkpoint 1, got %s", hist.Tasks["ingest"].Status)
		}
		if hist.Tasks["sidecar"].Status != engine.TaskStatusLeased {
			t.Fatalf("expected sidecar LEASED at checkpoint 1, got %s", hist.Tasks["sidecar"].Status)
		}
		if hist.Tasks["transform"].Status != engine.TaskStatusPending {
			t.Fatalf("expected transform PENDING at checkpoint 1, got %s", hist.Tasks["transform"].Status)
		}
	})

	t.Run("reconstructs intermediate historical state by 1-based checkpoint number", func(t *testing.T) {
		t.Parallel()
		hist, cp, err := engine.StateAtCheckpoint(dag, journal, "1")
		if err != nil {
			t.Fatalf("StateAtCheckpoint(1) failed: %v", err)
		}
		if cp.Name != "after_ingest" {
			t.Fatalf("expected checkpoint 1 name after_ingest, got %q", cp.Name)
		}
		if hist.Tasks["transform"].Status != engine.TaskStatusPending {
			t.Fatalf("expected transform PENDING at checkpoint 1, got %s", hist.Tasks["transform"].Status)
		}
	})

	t.Run("rejects unknown checkpoint number and name with ErrCheckpointNotFound", func(t *testing.T) {
		t.Parallel()
		for _, sel := range []string{"99", "nonexistent_cp", ""} {
			_, _, err := engine.StateAtCheckpoint(dag, journal, sel)
			if !errors.Is(err, engine.ErrCheckpointNotFound) {
				t.Fatalf("selector %q: expected ErrCheckpointNotFound, got %v", sel, err)
			}
		}
	})
}

func TestStateAtSeq(t *testing.T) {
	t.Parallel()

	dag, journal := buildCompletedTimeTravelRun(t)

	t.Run("inspects arbitrary event sequence", func(t *testing.T) {
		t.Parallel()
		// Seq 2 is TASK_LEASED for ingest; nothing is completed yet.
		hist, err := engine.StateAtSeq(dag, journal, 2)
		if err != nil {
			t.Fatalf("StateAtSeq(2) failed: %v", err)
		}
		if hist.LastSeq != 2 {
			t.Fatalf("expected LastSeq=2, got %d", hist.LastSeq)
		}
		if hist.Status != engine.RunStatusRunning {
			t.Fatalf("expected RUNNING at seq 2, got %s", hist.Status)
		}
		if hist.Tasks["ingest"].Status != engine.TaskStatusLeased {
			t.Fatalf("expected ingest LEASED at seq 2, got %s", hist.Tasks["ingest"].Status)
		}
		if hist.Tasks["sidecar"].Status != engine.TaskStatusPending {
			t.Fatalf("expected sidecar PENDING at seq 2, got %s", hist.Tasks["sidecar"].Status)
		}
		if hist.Tasks["transform"].Status != engine.TaskStatusPending {
			t.Fatalf("expected transform PENDING at seq 2, got %s", hist.Tasks["transform"].Status)
		}
	})

	t.Run("rejects invalid seq <= 0", func(t *testing.T) {
		t.Parallel()
		for _, seq := range []int64{0, -1, -100} {
			if _, err := engine.StateAtSeq(dag, journal, seq); err == nil {
				t.Fatalf("expected error for seq=%d, got nil", seq)
			}
		}
	})
}
