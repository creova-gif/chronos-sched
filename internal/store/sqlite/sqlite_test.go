package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

const testYAML = `
name: store-test
tasks:
  - id: step1
    handler: sim
    checkpoint_name: cp_one
  - id: step2
    handler: sim
    depends_on: [step1]
    checkpoint_name: cp_two
  - id: step3
    handler: sim
    depends_on: [step2]
`

func TestMultiHopForkLineageAndOptimisticConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	dag, err := engine.ParseDAG([]byte(testYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}
	now := time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

	// Create run_1 and complete step1 (checkpoint 1) + step2 (checkpoint 2).
	state1 := engine.NewRunState(dag, "run_1")
	startEvs, _, err := engine.Step(dag, state1, engine.CmdStartRun{RunID: "run_1"}, now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := engine.Apply(state1, startEvs[0]); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	if err := st.CreateRun(ctx, store.RunRecord{
		RunID:        "run_1",
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: testYAML,
		Status:       engine.RunStatusRunning,
		CreatedAtNs:  now.UnixNano(),
	}, startEvs); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	appendCmd := func(runID string, stState *engine.RunState, cmd engine.Command, dt time.Duration) {
		prevSeq := stState.LastSeq
		evs, _, err := engine.Step(dag, stState, cmd, now.Add(dt))
		if err != nil {
			t.Fatalf("Step(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(stState, ev); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
		if err := st.Append(ctx, runID, prevSeq, evs, stState.Status); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	appendCmd("run_1", state1, engine.CmdLeaseNextTask{WorkerID: "w-1"}, time.Millisecond)
	appendCmd("run_1", state1, engine.CmdCompleteTask{TaskID: "step1", WorkerID: "w-1", FencingToken: 1, Output: map[string]any{"s1": "a"}}, 2*time.Millisecond)
	cp1, err := engine.ResolveCheckpoint(state1, "cp_one")
	if err != nil {
		t.Fatalf("ResolveCheckpoint(cp_one): %v", err)
	}

	// Verify stale expectedLastSeq returns ErrSeqConflict.
	err = st.Append(ctx, "run_1", 1, []engine.Event{{RunID: "run_1", Seq: 2, Type: engine.EventTaskStarted}}, engine.RunStatusRunning)
	if !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("expected ErrSeqConflict, got %v", err)
	}

	// Fork run_2 from run_1 at cp_one, then complete step2 on run_2.
	fork2Evs, _, err := engine.Step(dag, state1, engine.CmdForkRun{
		NewRunID:           "run_2",
		CheckpointSelector: "cp_one",
		Overrides:          map[string]map[string]any{"step2": {"branch": "2"}},
	}, now.Add(3*time.Millisecond))
	if err != nil {
		t.Fatalf("fork run_2: %v", err)
	}
	if err := st.CreateRun(ctx, store.RunRecord{
		RunID:        "run_2",
		ParentRunID:  "run_1",
		ForkFromSeq:  cp1.Seq,
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: testYAML,
		Status:       engine.RunStatusRunning,
		CreatedAtNs:  now.Add(3 * time.Millisecond).UnixNano(),
	}, fork2Evs); err != nil {
		t.Fatalf("CreateRun run_2: %v", err)
	}

	evs2, err := st.LoadRunEvents(ctx, "run_2")
	if err != nil {
		t.Fatalf("LoadRunEvents run_2: %v", err)
	}
	state2, err := engine.Fold(dag, evs2)
	if err != nil {
		t.Fatalf("Fold run_2: %v", err)
	}
	appendCmd("run_2", state2, engine.CmdLeaseNextTask{WorkerID: "w-2"}, 4*time.Millisecond)
	appendCmd("run_2", state2, engine.CmdCompleteTask{TaskID: "step2", WorkerID: "w-2", FencingToken: state2.Tasks["step2"].FencingToken, Output: map[string]any{"s2": "b"}}, 5*time.Millisecond)

	// Multi-hop fork: run_3 forks from run_2 at cp_two!
	cp2, err := engine.ResolveCheckpoint(state2, "cp_two")
	if err != nil {
		t.Fatalf("ResolveCheckpoint(cp_two): %v", err)
	}
	fork3Evs, _, err := engine.Step(dag, state2, engine.CmdForkRun{
		NewRunID:           "run_3",
		CheckpointSelector: "cp_two",
		Overrides:          map[string]map[string]any{"step3": {"branch": "3"}},
	}, now.Add(6*time.Millisecond))
	if err != nil {
		t.Fatalf("fork run_3: %v", err)
	}
	if err := st.CreateRun(ctx, store.RunRecord{
		RunID:        "run_3",
		ParentRunID:  "run_2",
		ForkFromSeq:  cp2.Seq,
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: testYAML,
		Status:       engine.RunStatusRunning,
		CreatedAtNs:  now.Add(6 * time.Millisecond).UnixNano(),
	}, fork3Evs); err != nil {
		t.Fatalf("CreateRun run_3: %v", err)
	}

	// Open strictly read-only (offline CLI path) and verify multi-hop replay + write rejection.
	ro, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer func() { _ = ro.Close() }()

	if err := ro.Append(ctx, "run_3", cp2.Seq+1, []engine.Event{{RunID: "run_3", Seq: cp2.Seq + 2}}, engine.RunStatusRunning); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly, got %v", err)
	}
	evs3, err := ro.LoadRunEvents(ctx, "run_3")
	if err != nil {
		t.Fatalf("LoadRunEvents run_3: %v", err)
	}
	rep3, err := engine.VerifyReplay(dag, evs3)
	if err != nil {
		t.Fatalf("VerifyReplay run_3: %v", err)
	}
	if rep3.CompletedTasks != 2 || rep3.State.Tasks["step3"].Status != engine.TaskStatusPending {
		t.Fatalf("unexpected multi-hop replay state: %+v", rep3)
	}
}

func TestConcurrentWALReadsAndWritesNoSQLiteBusy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "concurrent.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	now := time.Now().UnixNano()
	const runs = 5
	for i := 0; i < runs; i++ {
		id := fmt.Sprintf("run_%d", i)
		if err := st.CreateRun(ctx, store.RunRecord{
			RunID:        id,
			WorkflowName: "conc",
			WorkflowHash: "abc",
			WorkflowYAML: testYAML,
			Status:       engine.RunStatusRunning,
			CreatedAtNs:  now,
		}, []engine.Event{{RunID: id, Seq: 1, TimestampNs: now, Type: engine.EventRunStarted}}); err != nil {
			t.Fatalf("CreateRun %s: %v", id, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, runs*2)
	for i := 0; i < runs; i++ {
		id := fmt.Sprintf("run_%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for seq := int64(2); seq <= 25; seq++ {
				if err := st.Append(ctx, id, seq-1, []engine.Event{{
					RunID: id, Seq: seq, TimestampNs: time.Now().UnixNano(), Type: engine.EventStaleFencingToken,
				}}, engine.RunStatusRunning); err != nil {
					errCh <- fmt.Errorf("writer %s seq=%d: %w", id, seq, err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for k := 0; k < 25; k++ {
				if _, err := st.LoadRunEvents(ctx, id); err != nil {
					errCh <- fmt.Errorf("reader %s iter=%d: %w", id, k, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
