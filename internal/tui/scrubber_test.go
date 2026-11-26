package tui_test

import (
	"encoding/json"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/tui"
)

func sampleScrubEvents() []engine.Event {
	cp1Payload, _ := json.Marshal(engine.CheckpointSavedPayload{
		CheckpointNum:  1,
		CheckpointName: "after_extract",
		TaskID:         "extract",
	})
	return []engine.Event{
		{RunID: "run_scrub_1", Seq: 1, Type: engine.EventRunStarted},
		{RunID: "run_scrub_1", Seq: 2, Type: engine.EventTaskCompleted, TaskID: "extract", WorkerID: "w-1"},
		{RunID: "run_scrub_1", Seq: 3, Type: engine.EventCheckpointSaved, TaskID: "extract", Payload: cp1Payload},
		{RunID: "run_scrub_1", Seq: 4, Type: engine.EventTaskStarted, TaskID: "load", WorkerID: "w-2"},
	}
}

func TestScrubModeToggleClampingAndHistoricalRender(t *testing.T) {
	t.Parallel()

	events := sampleScrubEvents()
	liveSnap := sampleSnapshot("run_scrub_1", nil)

	var scrubSeqs []int64
	m := tui.NewModel(tui.Config{
		RunID: "run_scrub_1",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return liveSnap, events, nil
		},
		OnScrubState: func(runID string, seq int64) (*engine.RunState, error) {
			if runID != "run_scrub_1" {
				t.Fatalf("expected run_scrub_1, got %q", runID)
			}
			scrubSeqs = append(scrubSeqs, seq)
			switch seq {
			case 1:
				return &engine.RunState{
					RunID:     runID,
					Status:    engine.RunStatusRunning,
					TopoOrder: []string{"extract", "transform", "load"},
					Tasks: map[string]*engine.TaskState{
						"extract":   {ID: "extract", Status: engine.TaskStatusPending},
						"transform": {ID: "transform", Status: engine.TaskStatusPending},
						"load":      {ID: "load", Status: engine.TaskStatusPending},
					},
				}, nil
			case 2, 3:
				return &engine.RunState{
					RunID:     runID,
					Status:    engine.RunStatusRunning,
					TopoOrder: []string{"extract", "transform", "load"},
					Tasks: map[string]*engine.TaskState{
						"extract":   {ID: "extract", Status: engine.TaskStatusCompleted, WorkerID: "w-hist"},
						"transform": {ID: "transform", Status: engine.TaskStatusPending},
						"load":      {ID: "load", Status: engine.TaskStatusPending},
					},
				}, nil
			default:
				return liveSnap.State, nil
			}
		},
	})

	// Before loading snapshot (no events), [s] does not enter scrub mode.
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	if !strings.Contains(next.(tui.Model).View(), "No events to scrub yet.") {
		t.Fatalf("expected empty events message when pressing [s] before snapshot:\n%s", next.(tui.Model).View())
	}

	m = loadInitialSnapshot(t, m)

	// Footer should advertise [s] Scrub.
	if !strings.Contains(m.View(), "[s] Scrub") {
		t.Fatalf("expected [s] Scrub in footer:\n%s", m.View())
	}

	// Enter scrub mode with [s] -> starts at last event (seq=4).
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "SCRUB MODE: seq 4/4 (TASK_STARTED)") {
		t.Fatalf("expected scrub banner at seq 4/4:\n%s", m.View())
	}

	// Step right / ']' at upper bound clamps to len(events)-1 (seq=4).
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{']'}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "SCRUB MODE: seq 4/4 (TASK_STARTED)") {
		t.Fatalf("expected upper bound clamp at seq 4/4:\n%s", m.View())
	}

	// Step backward with '[' and 'left' to seq=2, verify historical scrubState renders.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'['}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m = next.(tui.Model)
	histView := m.View()
	if !strings.Contains(histView, "SCRUB MODE: seq 2/4 (TASK_COMPLETED)") {
		t.Fatalf("expected scrub banner at seq 2/4:\n%s", histView)
	}
	if !strings.Contains(histView, "w-hist") {
		t.Fatalf("expected historical scrubState worker w-hist in DAG pane:\n%s", histView)
	}

	// Step backward past 0 clamps cleanly at seq=1.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'['}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "SCRUB MODE: seq 1/4 (RUN_STARTED)") {
		t.Fatalf("expected lower bound clamp at seq 1/4:\n%s", m.View())
	}

	// Exit scrub mode with [s] -> live snap.State is preserved.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	m = next.(tui.Model)
	liveView := m.View()
	if strings.Contains(liveView, "SCRUB MODE:") {
		t.Fatalf("expected scrub mode exited on [s]:\n%s", liveView)
	}
	if !strings.Contains(liveView, "[run] load") || !strings.Contains(liveView, "w-2") {
		t.Fatalf("expected live snap.State preserved after exiting scrub mode:\n%s", liveView)
	}

	// Enter again and exit with [esc].
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = next.(tui.Model)
	if strings.Contains(m.View(), "SCRUB MODE:") {
		t.Fatalf("expected scrub mode exited on [esc]:\n%s", m.View())
	}

	if len(scrubSeqs) == 0 || scrubSeqs[0] != 4 {
		t.Fatalf("expected OnScrubState invoked starting at seq 4, got %v", scrubSeqs)
	}
}

func TestScrubSessionHelpers(t *testing.T) {
	t.Parallel()

	events := sampleScrubEvents()
	sess := tui.NewScrubSession(events)
	if sess.Cursor() != len(events)-1 {
		t.Fatalf("expected initial cursor %d, got %d", len(events)-1, sess.Cursor())
	}

	if idx := tui.FindCheckpointIndex(events, 1); idx != 2 {
		t.Fatalf("expected checkpoint 1 at index 2, got %d", idx)
	}
	if idx := tui.FindCheckpointIndex(events, 99); idx != -1 {
		t.Fatalf("expected missing checkpoint to return -1, got %d", idx)
	}

	dag, err := engine.BuildDAG(engine.WorkflowSpec{
		Name: "scrub-dag",
		Tasks: []engine.TaskSpec{
			{ID: "extract", Handler: "echo_extract"},
			{ID: "load", Handler: "echo_load", DependsOn: []string{"extract"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("BuildDAG: %v", err)
	}

	sess.Step(-2) // cursor = 1 (seq 2: extract completed)
	st, err := sess.ComputeDisplayState(dag)
	if err != nil {
		t.Fatalf("ComputeDisplayState: %v", err)
	}
	if st.Tasks["extract"].Status != engine.TaskStatusCompleted {
		t.Fatalf("expected extract completed at seq 2, got %s", st.Tasks["extract"].Status)
	}
	if st.Tasks["load"].Status != engine.TaskStatusPending {
		t.Fatalf("expected load pending at seq 2, got %s", st.Tasks["load"].Status)
	}
}
