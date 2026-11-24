package engine_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

const sampleWorkflowYAML = `
name: sample-pipeline
tasks:
  - id: fetch
    handler: sim
    priority: 10
    max_retries: 2
    checkpoint_name: after_fetch
    params:
      source: prod
  - id: transform
    handler: sim
    depends_on: [fetch]
    max_retries: 2
    params:
      mode: standard
  - id: enrich
    handler: sim
    depends_on: [fetch]
    max_retries: 1
  - id: aggregate
    handler: sim
    depends_on: [transform, enrich]
    max_retries: 1
`

func TestParseDAGValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "valid DAG",
			yaml: sampleWorkflowYAML,
		},
		{
			name:    "cycle detected",
			yaml:    "name: cyc\ntasks:\n  - id: a\n    handler: sim\n    depends_on: [b]\n  - id: b\n    handler: sim\n    depends_on: [a]\n",
			wantErr: "dependency cycle",
		},
		{
			name:    "missing dependency",
			yaml:    "name: miss\ntasks:\n  - id: a\n    handler: sim\n    depends_on: [ghost]\n",
			wantErr: "unknown task",
		},
		{
			name:    "self dependency",
			yaml:    "name: self\ntasks:\n  - id: a\n    handler: sim\n    depends_on: [a]\n",
			wantErr: "depend on itself",
		},
		{
			name:    "duplicate task id",
			yaml:    "name: dup\ntasks:\n  - id: a\n    handler: sim\n  - id: a\n    handler: sim\n",
			wantErr: "duplicate task id",
		},
		{
			name:    "duplicate checkpoint_name",
			yaml:    "name: dupcp\ntasks:\n  - id: a\n    handler: sim\n    checkpoint_name: cp1\n  - id: b\n    handler: sim\n    checkpoint_name: cp1\n",
			wantErr: "duplicate checkpoint_name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dag, err := engine.ParseDAG([]byte(tc.yaml))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(dag.TopoOrder) != 4 || dag.TopoOrder[0] != "fetch" || dag.TopoOrder[3] != "aggregate" {
				t.Fatalf("unexpected topo order: %v", dag.TopoOrder)
			}
		})
	}
}

func TestLeaseExpiryAmbiguousOutcomeAndZombieRejection(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	state := engine.NewRunState(dag, "run_001")
	var journal []engine.Event

	applyStep := func(cmd engine.Command, ts time.Time) ([]engine.Event, error) {
		evs, _, err := engine.Step(dag, state, cmd, ts)
		for _, ev := range evs {
			if applyErr := engine.Apply(state, ev); applyErr != nil {
				t.Fatalf("Apply(%s): %v", ev.Type, applyErr)
			}
			journal = append(journal, ev)
		}
		return evs, err
	}

	if _, err := applyStep(engine.CmdStartRun{RunID: "run_001"}, now); err != nil {
		t.Fatalf("start run: %v", err)
	}

	// Worker w-1 leases fetch (token 1), starts it, then dies (SIGKILL).
	evs, err := applyStep(engine.CmdLeaseNextTask{WorkerID: "w-1", LeaseTTL: engine.DefaultLeaseTTL}, now)
	if err != nil || len(evs) != 1 || evs[0].TaskID != "fetch" || evs[0].FencingToken != 1 {
		t.Fatalf("unexpected lease events: %+v err=%v", evs, err)
	}
	if _, err := applyStep(engine.CmdMarkTaskStarted{TaskID: "fetch", WorkerID: "w-1", FencingToken: 1}, now.Add(100*time.Millisecond)); err != nil {
		t.Fatalf("mark started: %v", err)
	}

	// Advance clock past 3s TTL -> reaper fires LEASE_EXPIRED + TASK_OUTCOME_AMBIGUOUS.
	expiredEvs, err := applyStep(engine.CmdExpireLeases{}, now.Add(3*time.Second+250*time.Millisecond))
	if err != nil {
		t.Fatalf("expire leases: %v", err)
	}
	if len(expiredEvs) != 2 || expiredEvs[0].Type != engine.EventLeaseExpired || expiredEvs[1].Type != engine.EventTaskOutcomeAmbiguous {
		t.Fatalf("expected LEASE_EXPIRED + TASK_OUTCOME_AMBIGUOUS, got %+v", expiredEvs)
	}
	if state.Tasks["fetch"].Status != engine.TaskStatusPending || state.Tasks["fetch"].AmbiguousCount != 1 {
		t.Fatalf("expected fetch back to PENDING with ambiguous=1, got %+v", state.Tasks["fetch"])
	}

	// Worker w-2 leases fetch with monotonic fencing_token=2.
	reLeaseEvs, err := applyStep(engine.CmdLeaseNextTask{WorkerID: "w-2", LeaseTTL: engine.DefaultLeaseTTL}, now.Add(3500*time.Millisecond))
	if err != nil || len(reLeaseEvs) != 1 || reLeaseEvs[0].FencingToken != 2 || reLeaseEvs[0].Attempt != 2 {
		t.Fatalf("expected attempt 2 token 2, got %+v err=%v", reLeaseEvs, err)
	}

	// Zombie worker w-1 wakes up and tries to complete with stale token 1 -> MUST reject.
	staleEvs, err := applyStep(engine.CmdCompleteTask{
		TaskID:       "fetch",
		WorkerID:     "w-1",
		FencingToken: 1,
		Output:       map[string]any{"stale": true},
	}, now.Add(3600*time.Millisecond))
	if !errors.Is(err, engine.ErrStaleFencingToken) {
		t.Fatalf("expected ErrStaleFencingToken, got %v", err)
	}
	if len(staleEvs) != 1 || staleEvs[0].Type != engine.EventStaleFencingToken {
		t.Fatalf("expected STALE_FENCING_TOKEN audit event, got %+v", staleEvs)
	}
	if state.Tasks["fetch"].Status == engine.TaskStatusCompleted {
		t.Fatalf("zombie worker corrupted task state")
	}

	// Healthy worker w-2 completes fetch with token 2 -> TASK_COMPLETED + CHECKPOINT_SAVED.
	doneEvs, err := applyStep(engine.CmdCompleteTask{
		TaskID:       "fetch",
		WorkerID:     "w-2",
		FencingToken: 2,
		Output:       map[string]any{"rows": 42},
	}, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("w-2 complete: %v", err)
	}
	if len(doneEvs) != 2 || doneEvs[0].Type != engine.EventTaskCompleted || doneEvs[1].Type != engine.EventCheckpointSaved {
		t.Fatalf("expected TASK_COMPLETED + CHECKPOINT_SAVED, got %+v", doneEvs)
	}

	// Replay verifies 100% deterministic state reconstruction.
	rep, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if rep.AmbiguousEvents != 1 || rep.Checkpoints != 1 || rep.CompletedTasks != 1 {
		t.Fatalf("unexpected replay report: %+v", rep)
	}
}

func TestForkClearsInFlightLeasesRejectsCompletedOverrideAndDiffs(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	runA := engine.NewRunState(dag, "run_001")
	var eventsA []engine.Event

	stepA := func(cmd engine.Command, dt time.Duration) {
		evs, _, err := engine.Step(dag, runA, cmd, now.Add(dt))
		if err != nil {
			t.Fatalf("stepA(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(runA, ev); err != nil {
				t.Fatalf("applyA: %v", err)
			}
			eventsA = append(eventsA, ev)
		}
	}

	stepA(engine.CmdStartRun{RunID: "run_001"}, 0)
	stepA(engine.CmdLeaseNextTask{WorkerID: "w-1"}, 10*time.Millisecond)
	stepA(engine.CmdCompleteTask{TaskID: "fetch", WorkerID: "w-1", FencingToken: 1, Output: map[string]any{"v": 1}}, 20*time.Millisecond)
	// Checkpoint 1 ("after_fetch") is now saved! Next, lease transform so it's in-flight on run_001.
	stepA(engine.CmdLeaseNextTask{WorkerID: "w-2"}, 30*time.Millisecond) // leases enrich or transform
	// Reject override targeting already-completed "fetch" at checkpoint "after_fetch".
	_, _, err = engine.Step(dag, runA, engine.CmdForkRun{
		NewRunID:           "run_bad",
		CheckpointSelector: "after_fetch",
		Overrides:          map[string]map[string]any{"fetch": {"source": "staging"}},
	}, now.Add(40*time.Millisecond))
	if !errors.Is(err, engine.ErrOverrideCompletedTask) {
		t.Fatalf("expected ErrOverrideCompletedTask, got %v", err)
	}

	// Fork at checkpoint 1 ("after_fetch") with override on downstream "transform".
	forkEvs, _, err := engine.Step(dag, runA, engine.CmdForkRun{
		NewRunID:           "run_002",
		CheckpointSelector: "after_fetch",
		Overrides:          map[string]map[string]any{"transform": {"mode": "fast"}},
	}, now.Add(50*time.Millisecond))
	if err != nil || len(forkEvs) != 1 || forkEvs[0].Type != engine.EventTimelineForked {
		t.Fatalf("unexpected fork result: evs=%+v err=%v", forkEvs, err)
	}

	// Reference-by-lineage prefix: eventsA[0:cpSeq] + forkEvs.
	cp, err := engine.ResolveCheckpoint(runA, "1")
	if err != nil {
		t.Fatalf("ResolveCheckpoint(1): %v", err)
	}
	eventsB := append([]engine.Event(nil), eventsA[:cp.Seq]...)
	eventsB = append(eventsB, forkEvs...)
	runB, err := engine.Fold(dag, eventsB)
	if err != nil {
		t.Fatalf("Fold runB: %v", err)
	}

	// Upstream "fetch" remains COMPLETED; downstream tasks are PENDING (no leaked leases).
	if runB.Tasks["fetch"].Status != engine.TaskStatusCompleted {
		t.Fatalf("expected fetch COMPLETED on runB, got %s", runB.Tasks["fetch"].Status)
	}
	if runB.Tasks["transform"].Status != engine.TaskStatusPending || runB.Tasks["enrich"].Status != engine.TaskStatusPending {
		t.Fatalf("expected downstream tasks PENDING on runB, got transform=%s enrich=%s", runB.Tasks["transform"].Status, runB.Tasks["enrich"].Status)
	}
	in := engine.ResolveTaskInput(dag, runB, "transform")
	if in["mode"] != "fast" {
		t.Fatalf("expected overridden mode=fast, got %v", in["mode"])
	}

	// Complete transform on runB with different output and diff against runA.
	stepB := func(cmd engine.Command, dt time.Duration) {
		evs, _, err := engine.Step(dag, runB, cmd, now.Add(dt))
		if err != nil {
			t.Fatalf("stepB(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(runB, ev); err != nil {
				t.Fatalf("applyB: %v", err)
			}
			eventsB = append(eventsB, ev)
		}
	}
	stepB(engine.CmdLeaseNextTask{WorkerID: "w-3"}, 60*time.Millisecond)
	leasedID := eventsB[len(eventsB)-1].TaskID
	leasedTok := eventsB[len(eventsB)-1].FencingToken
	stepB(engine.CmdCompleteTask{TaskID: leasedID, WorkerID: "w-3", FencingToken: leasedTok, Output: map[string]any{"branch": "b"}}, 70*time.Millisecond)

	diff, err := engine.DiffRuns(dag, eventsA, eventsB)
	if err != nil {
		t.Fatalf("DiffRuns: %v", err)
	}
	if diff.CommonPrefixSeq != cp.Seq {
		t.Fatalf("expected common prefix seq %d, got %d", cp.Seq, diff.CommonPrefixSeq)
	}
	if len(diff.TaskDeltas) == 0 {
		t.Fatalf("expected non-empty task deltas")
	}
	formatted := engine.FormatDiff(diff)
	if !strings.Contains(formatted, "run_001 vs run_002") {
		t.Fatalf("unexpected diff output:\n%s", formatted)
	}
}

func TestDeterministic100TaskDAGReplay(t *testing.T) {
	t.Parallel()

	tasks := make([]engine.TaskSpec, 100)
	for i := 0; i < 100; i++ {
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("task_%03d", (i-1)/2)}
		}
		tasks[i] = engine.TaskSpec{
			ID:         fmt.Sprintf("task_%03d", i),
			Handler:    "sim",
			DependsOn:  deps,
			MaxRetries: 1,
		}
	}
	dag, err := engine.BuildDAG(engine.WorkflowSpec{Name: "bench-100", Tasks: tasks}, []byte("bench-100"))
	if err != nil {
		t.Fatalf("BuildDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	state := engine.NewRunState(dag, "run_100")
	var events []engine.Event

	runCmd := func(cmd engine.Command, dt time.Duration) []engine.Event {
		evs, _, err := engine.Step(dag, state, cmd, now.Add(dt))
		if err != nil {
			t.Fatalf("Step(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			events = append(events, ev)
		}
		return evs
	}

	runCmd(engine.CmdStartRun{RunID: "run_100"}, 0)
	stepIdx := 1
	for state.Status == engine.RunStatusRunning {
		evs := runCmd(engine.CmdLeaseNextTask{WorkerID: fmt.Sprintf("w-%d", (stepIdx%5)+1)}, time.Duration(stepIdx)*time.Millisecond)
		if len(evs) == 0 {
			t.Fatalf("deadlock: run still RUNNING but no ready tasks")
		}
		ev := evs[0]
		runCmd(engine.CmdCompleteTask{
			TaskID:       ev.TaskID,
			WorkerID:     ev.WorkerID,
			FencingToken: ev.FencingToken,
			Output:       map[string]any{"ok": true},
		}, time.Duration(stepIdx)*time.Millisecond+500*time.Microsecond)
		stepIdx++
	}

	rep, err := engine.VerifyReplay(dag, events)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if rep.FinalStatus != engine.RunStatusCompleted || rep.CompletedTasks != 100 || rep.Checkpoints != 100 {
		t.Fatalf("unexpected 100-task replay report: %+v", rep)
	}
}
