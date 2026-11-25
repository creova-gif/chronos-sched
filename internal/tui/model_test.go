package tui_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/tui"
)

func loadInitialSnapshot(t *testing.T, m tui.Model) tui.Model {
	t.Helper()
	initCmd := m.Init()
	if initCmd == nil {
		t.Fatalf("expected Init() to return a batch command")
	}
	batchMsg, ok := initCmd().(tea.BatchMsg)
	if !ok || len(batchMsg) == 0 {
		t.Fatalf("expected tea.BatchMsg from Init(), got %T", initCmd())
	}
	snapMsg := batchMsg[0]()
	next, _ := m.Update(snapMsg)
	return next.(tui.Model)
}

func sampleSnapshot(runID string, workers []runtime.WorkerInfo) runtime.RunSnapshotResponse {
	return runtime.RunSnapshotResponse{
		State: &engine.RunState{
			RunID:     runID,
			Status:    engine.RunStatusRunning,
			TopoOrder: []string{"extract", "transform", "load"},
			Tasks: map[string]*engine.TaskState{
				"extract":   {ID: "extract", Status: engine.TaskStatusCompleted, WorkerID: "w-1"},
				"transform": {ID: "transform", Status: engine.TaskStatusCompleted, WorkerID: "w-2"},
				"load":      {ID: "load", Status: engine.TaskStatusRunning, WorkerID: "w-1"},
			},
			Checkpoints: []engine.CheckpointInfo{
				{Number: 1, Name: "after_extract", Seq: 4, TaskID: "extract"},
				{Number: 2, Name: "after_transform", Seq: 7, TaskID: "transform"},
			},
		},
		Workers: workers,
	}
}

func TestTUIKillDisabledInReadOnlyInspectAndEnabledInDemo(t *testing.T) {
	t.Parallel()

	killed := false
	roModel := tui.NewModel(tui.Config{
		RunID:     "run_001",
		AllowKill: false,
		OnKillWorker: func() string {
			killed = true
			return "killed"
		},
	})
	next, _ := roModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if killed {
		t.Fatalf("expected [k] to be disabled in read-only inspect mode")
	}
	view := next.(tui.Model).View()
	if !strings.Contains(view, "read-only") {
		t.Fatalf("expected read-only indicator in view:\n%s", view)
	}

	workerAlive := true
	demoModel := tui.NewModel(tui.Config{
		RunID:     "run_001",
		AllowKill: true,
		OnKillWorker: func() string {
			killed = true
			workerAlive = false
			return "Killed w-1"
		},
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return sampleSnapshot(runID, []runtime.WorkerInfo{
				{WorkerID: "w-1", PID: 4242, Alive: workerAlive},
				{WorkerID: "w-2", PID: 4343, Alive: true},
			}), nil, nil
		},
	})
	demoModel = loadInitialSnapshot(t, demoModel)
	beforeKillView := demoModel.View()
	if !strings.Contains(beforeKillView, "w-1(pid=4242)") || !strings.Contains(beforeKillView, "ALIVE") {
		t.Fatalf("expected worker strip with ALIVE badge before kill:\n%s", beforeKillView)
	}

	nextDemo, cmd := demoModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if !killed {
		t.Fatalf("expected [k] to invoke OnKillWorker in demo mode")
	}
	if cmd != nil {
		nextDemo, _ = nextDemo.(tui.Model).Update(cmd())
	}
	demoView := nextDemo.(tui.Model).View()
	if !strings.Contains(demoView, "Killed w-1") {
		t.Fatalf("expected kill status in demo view:\n%s", demoView)
	}
	if !strings.Contains(demoView, "DEAD") {
		t.Fatalf("expected killed worker to display DEAD badge in workers strip:\n%s", demoView)
	}
}

func TestTUIForkCheckpointPickerOverrideEnterAndEscCancel(t *testing.T) {
	t.Parallel()

	var (
		gotRunID    string
		gotCP       int
		gotOverride string
		forkCalls   int
	)
	m := tui.NewModel(tui.Config{
		RunID: "run_orig",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return sampleSnapshot(runID, nil), nil, nil
		},
		OnForkWithOverride: func(runID string, checkpointNum int, override string) (string, error) {
			forkCalls++
			gotRunID = runID
			gotCP = checkpointNum
			gotOverride = override
			return "run_fork_1", nil
		},
	})
	m = loadInitialSnapshot(t, m)

	// Open picker with [f] and cancel with [esc].
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	m = next.(tui.Model)
	modalView := m.View()
	if !strings.Contains(modalView, "FORK CHECKPOINT PICKER") ||
		!strings.Contains(modalView, "after_extract") ||
		!strings.Contains(modalView, "after_transform") {
		t.Fatalf("expected fork modal listing checkpoints:\n%s", modalView)
	}

	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = next.(tui.Model)
	if forkCalls != 0 {
		t.Fatalf("expected esc cancel to not invoke fork, got %d calls", forkCalls)
	}
	if !strings.Contains(m.View(), "Fork cancelled.") {
		t.Fatalf("expected Fork cancelled status:\n%s", m.View())
	}

	// Open picker again, navigate down with [j], navigate up with [k], navigate down with KeyDown to cp=2.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = next.(tui.Model)

	// Type override string containing 'k' and test backspace.
	for _, r := range "load.mode=fastX" {
		next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = next.(tui.Model)
	}
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	m = next.(tui.Model)

	if !strings.Contains(m.View(), "load.mode=fast_") {
		t.Fatalf("expected override input in modal view:\n%s", m.View())
	}

	// Press enter to execute fork.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(tui.Model)
	if forkCalls != 1 {
		t.Fatalf("expected 1 fork call, got %d", forkCalls)
	}
	if gotRunID != "run_orig" || gotCP != 2 || gotOverride != "load.mode=fast" {
		t.Fatalf("unexpected fork args: runID=%q cp=%d override=%q", gotRunID, gotCP, gotOverride)
	}
	if !strings.Contains(m.View(), "Forked run_fork_1") {
		t.Fatalf("expected fork status in view:\n%s", m.View())
	}

	// Verify fallback to OnFork when OnForkWithOverride is nil.
	fallbackCP := 0
	fbModel := tui.NewModel(tui.Config{
		RunID: "run_orig",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return sampleSnapshot(runID, nil), nil, nil
		},
		OnFork: func(_ string, checkpointNum int) (string, error) {
			fallbackCP = checkpointNum
			return "run_fork_fb", nil
		},
	})
	fbModel = loadInitialSnapshot(t, fbModel)
	next, _ = fbModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	next, _ = next.(tui.Model).Update(tea.KeyMsg{Type: tea.KeyEnter})
	if fallbackCP != 1 {
		t.Fatalf("expected fallback OnFork with checkpoint 1, got %d", fallbackCP)
	}
	if !strings.Contains(next.(tui.Model).View(), "run_fork_fb") {
		t.Fatalf("expected fallback forked run in status:\n%s", next.(tui.Model).View())
	}
}

func TestTUIBranchSwitcherCyclesBetweenOriginalAndForkedRuns(t *testing.T) {
	t.Parallel()

	var fetchedRuns []string
	m := tui.NewModel(tui.Config{
		RunID: "run_orig",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			fetchedRuns = append(fetchedRuns, runID)
			return sampleSnapshot(runID, nil), nil, nil
		},
		OnFork: func(_ string, _ int) (string, error) {
			return "run_fork_1", nil
		},
	})
	m = loadInitialSnapshot(t, m)

	// Pressing [b] before any fork reports no forked branches.
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "No forked branches yet") {
		t.Fatalf("expected message when no forked branches exist:\n%s", m.View())
	}

	// Fork a branch.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	next, _ = next.(tui.Model).Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(tui.Model)

	// Switch to forked branch with [b].
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	m = next.(tui.Model)
	if cmd == nil {
		t.Fatalf("expected fetch cmd on branch switch")
	}
	next, _ = m.Update(cmd())
	m = next.(tui.Model)
	view := m.View()
	if !strings.Contains(view, "[run_fork_1 |") || !strings.Contains(view, "branches: 2") {
		t.Fatalf("expected active branch switched to run_fork_1:\n%s", view)
	}

	// Switch back to original branch with [b].
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'b'}})
	m = next.(tui.Model)
	if cmd != nil {
		next, _ = m.Update(cmd())
		m = next.(tui.Model)
	}
	if !strings.Contains(m.View(), "[run_orig |") {
		t.Fatalf("expected active branch cycled back to run_orig:\n%s", m.View())
	}
	if len(fetchedRuns) < 3 || fetchedRuns[len(fetchedRuns)-2] != "run_fork_1" || fetchedRuns[len(fetchedRuns)-1] != "run_orig" {
		t.Fatalf("expected fetches for run_fork_1 then run_orig, got %v", fetchedRuns)
	}
}

func TestTUIReplayCallbackStatusDisplay(t *testing.T) {
	t.Parallel()

	var replayedRun string
	m := tui.NewModel(tui.Config{
		RunID: "run_replay_1",
		OnReplay: func(runID string) (string, error) {
			replayedRun = runID
			return "Replay PASS: deterministic state matches (12 events)", nil
		},
	})

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	m = next.(tui.Model)
	if replayedRun != "run_replay_1" {
		t.Fatalf("expected OnReplay called with run_replay_1, got %q", replayedRun)
	}
	if !strings.Contains(m.View(), "Replay PASS: deterministic state matches (12 events)") {
		t.Fatalf("expected replay result in status bar:\n%s", m.View())
	}
}

func TestStreamEventsParsesSSEAndCancelsCleanly(t *testing.T) {
	t.Parallel()

	msg1 := runtime.SSEMessage{
		Event: engine.Event{
			RunID:  "run_sse",
			Seq:    1,
			Type:   engine.EventRunStarted,
			TaskID: "",
		},
		RunStatus: engine.RunStatusRunning,
	}
	msg2 := runtime.SSEMessage{
		Event: engine.Event{
			RunID:  "run_sse",
			Seq:    2,
			Type:   engine.EventTaskCompleted,
			TaskID: "extract",
		},
		RunStatus: engine.RunStatusRunning,
	}

	b1, err := json.Marshal(msg1)
	if err != nil {
		t.Fatalf("marshal msg1: %v", err)
	}
	b2, err := json.Marshal(msg2)
	if err != nil {
		t.Fatalf("marshal msg2: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintf(w, ": keep-alive\n\n")
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b1)
		flusher.Flush()
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b2)
		flusher.Flush()
		// Hold stream open until client cancels context.
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan runtime.SSEMessage, 4)
	errCh := make(chan error, 1)
	go func() {
		errCh <- tui.StreamEvents(ctx, srv.URL, out)
	}()

	var received []runtime.SSEMessage
	timeout := time.After(3 * time.Second)
	for len(received) < 2 {
		select {
		case msg := <-out:
			received = append(received, msg)
		case <-timeout:
			t.Fatalf("timed out waiting for SSE messages, got %d", len(received))
		}
	}

	if received[0].Event.Seq != 1 || received[0].Event.Type != engine.EventRunStarted {
		t.Fatalf("unexpected first SSE message: %+v", received[0])
	}
	if received[1].Event.Seq != 2 || received[1].Event.TaskID != "extract" {
		t.Fatalf("unexpected second SSE message: %+v", received[1])
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected clean nil return on context cancel, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("StreamEvents did not exit cleanly after context cancel")
	}
}

func TestTUIPauseResumeToggle(t *testing.T) {
	t.Parallel()

	status := engine.RunStatusRunning
	var calls []bool
	m := tui.NewModel(tui.Config{
		RunID: "run_pause_1",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			snap := sampleSnapshot(runID, nil)
			snap.State.Status = status
			return snap, nil, nil
		},
		OnPauseResume: func(runID string, pause bool) (string, error) {
			if runID != "run_pause_1" {
				t.Fatalf("expected run_pause_1, got %q", runID)
			}
			calls = append(calls, pause)
			if pause {
				status = engine.RunStatusPaused
				return "Run paused.", nil
			}
			status = engine.RunStatusRunning
			return "Run resumed.", nil
		},
	})
	m = loadInitialSnapshot(t, m)

	// First [p] when RUNNING -> pause=true.
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "Run paused.") {
		t.Fatalf("expected pause status in view:\n%s", m.View())
	}
	if cmd != nil {
		next, _ = m.Update(cmd())
		m = next.(tui.Model)
	}

	// Second [p] when PAUSED -> pause=false.
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	m = next.(tui.Model)
	if !strings.Contains(m.View(), "Run resumed.") {
		t.Fatalf("expected resume status in view:\n%s", m.View())
	}
	if cmd != nil {
		next, _ = m.Update(cmd())
		_ = next.(tui.Model)
	}

	if len(calls) != 2 || !calls[0] || calls[1] {
		t.Fatalf("expected pause calls [true, false], got %v", calls)
	}
}

func TestTUICancelRequiresConfirmation(t *testing.T) {
	t.Parallel()

	cancelCalls := 0
	var cancelledRun string
	m := tui.NewModel(tui.Config{
		RunID: "run_cancel_1",
		OnCancel: func(runID string) (string, error) {
			cancelCalls++
			cancelledRun = runID
			return "Run run_cancel_1 cancelled.", nil
		},
	})

	// Pressing [x] prompts for confirmation without calling OnCancel.
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	m = next.(tui.Model)
	if cancelCalls != 0 {
		t.Fatalf("expected 0 cancel calls before confirmation, got %d", cancelCalls)
	}
	if !strings.Contains(m.View(), "Cancel run run_cancel_1? Press [y] to confirm or [n]/[esc] to abort.") {
		t.Fatalf("expected cancel confirmation prompt in view:\n%s", m.View())
	}

	// Pressing [n] aborts without calling OnCancel.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	m = next.(tui.Model)
	if cancelCalls != 0 {
		t.Fatalf("expected 0 cancel calls after [n] abort, got %d", cancelCalls)
	}
	if !strings.Contains(m.View(), "Cancel aborted.") {
		t.Fatalf("expected Cancel aborted status in view:\n%s", m.View())
	}

	// Pressing [x] then [y] invokes OnCancel.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	m = next.(tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	m = next.(tui.Model)
	if cancelCalls != 1 || cancelledRun != "run_cancel_1" {
		t.Fatalf("expected 1 cancel call for run_cancel_1, got calls=%d run=%q", cancelCalls, cancelledRun)
	}
	if !strings.Contains(m.View(), "Run run_cancel_1 cancelled.") {
		t.Fatalf("expected cancelled status in view:\n%s", m.View())
	}
}

func TestTUITraceExportDisplaysStatus(t *testing.T) {
	t.Parallel()

	var exportedRun string
	m := tui.NewModel(tui.Config{
		RunID: "run_trace_1",
		OnTraceExport: func(runID string) (string, error) {
			exportedRun = runID
			return "/tmp/run_trace_1.perfetto.json", nil
		},
	})

	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	m = next.(tui.Model)
	if exportedRun != "run_trace_1" {
		t.Fatalf("expected OnTraceExport called with run_trace_1, got %q", exportedRun)
	}
	if !strings.Contains(m.View(), "/tmp/run_trace_1.perfetto.json") {
		t.Fatalf("expected trace path in status bar:\n%s", m.View())
	}
}

func TestTUITaskFilterNarrowsDAGPaneAndEscClears(t *testing.T) {
	t.Parallel()

	m := tui.NewModel(tui.Config{
		RunID: "run_filter_1",
		Fetch: func(runID string) (runtime.RunSnapshotResponse, []engine.Event, error) {
			return sampleSnapshot(runID, nil), nil, nil
		},
	})
	m = loadInitialSnapshot(t, m)

	// Open filter mode with [/] and type "transform".
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = next.(tui.Model)
	for _, r := range "transform" {
		next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = next.(tui.Model)
	}

	filteredView := m.View()
	if !strings.Contains(filteredView, "transform") {
		t.Fatalf("expected matching task transform in filtered view:\n%s", filteredView)
	}
	if strings.Contains(filteredView, "extract") || strings.Contains(filteredView, "load") {
		t.Fatalf("expected non-matching tasks extract/load to be hidden in filtered view:\n%s", filteredView)
	}

	// Lock filter with [enter], verify filter persists.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(tui.Model)
	lockedView := m.View()
	if !strings.Contains(lockedView, "transform") || strings.Contains(lockedView, "extract") {
		t.Fatalf("expected locked filter to remain active after enter:\n%s", lockedView)
	}

	// Clear filter with [esc], verify all tasks return.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = next.(tui.Model)
	clearedView := m.View()
	if !strings.Contains(clearedView, "extract") || !strings.Contains(clearedView, "transform") || !strings.Contains(clearedView, "load") {
		t.Fatalf("expected all tasks restored after esc:\n%s", clearedView)
	}

	// Also verify filtering by WorkerID ("w-2" only has transform).
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = next.(tui.Model)
	for _, r := range "w-2" {
		next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = next.(tui.Model)
	}
	workerFiltered := m.View()
	if !strings.Contains(workerFiltered, "transform") || strings.Contains(workerFiltered, "extract") || strings.Contains(workerFiltered, "load") {
		t.Fatalf("expected WorkerID filter w-2 to show only transform:\n%s", workerFiltered)
	}
}
