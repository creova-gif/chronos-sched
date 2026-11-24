package engine_test

import (
	"encoding/json"
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
		{
			name:    "negative resource value",
			yaml:    "name: negres\ntasks:\n  - id: a\n    handler: sim\n    resources:\n      cpu: -1\n",
			wantErr: "negative resource",
		},
		{
			name:    "negative deadline_ms",
			yaml:    "name: negdl\ntasks:\n  - id: a\n    handler: sim\n    deadline_ms: -10\n",
			wantErr: "negative deadline_ms",
		},
		{
			name:    "negative retry_backoff_ms",
			yaml:    "name: negbo\ntasks:\n  - id: a\n    handler: sim\n    retry_backoff_ms: -5\n",
			wantErr: "negative retry_backoff_ms",
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

func TestResourceConstrainedLeasing(t *testing.T) {
	t.Parallel()

	const yamlSpec = `
name: resource-constrained
tasks:
  - id: heavy
    handler: sim
    priority: 10
    resources:
      cpu: 4
  - id: light
    handler: sim
    priority: 5
    resources:
      cpu: 2
`
	dag, err := engine.ParseDAG([]byte(yamlSpec))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	state := engine.NewRunState(dag, "run_res")
	var journal []engine.Event

	applyStep := func(cmd engine.Command, ts time.Time) []engine.Event {
		evs, _, err := engine.Step(dag, state, cmd, ts)
		if err != nil {
			t.Fatalf("Step(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply(%s): %v", ev.Type, err)
			}
			journal = append(journal, ev)
		}
		return evs
	}

	applyStep(engine.CmdStartRun{RunID: "run_res"}, now)

	// Worker with cpu=2 cannot lease heavy (cpu=4), so it leases light (cpu=2).
	evs := applyStep(engine.CmdLeaseNextTask{
		WorkerID:           "w-small",
		AvailableResources: map[string]int{"cpu": 2},
	}, now.Add(10*time.Millisecond))
	if len(evs) != 1 || evs[0].TaskID != "light" {
		t.Fatalf("expected worker with cpu=2 to lease light, got %+v", evs)
	}

	// Only heavy remains pending; worker with cpu=2 cannot lease it.
	evs = applyStep(engine.CmdLeaseNextTask{
		WorkerID:           "w-small-2",
		AvailableResources: map[string]int{"cpu": 2},
	}, now.Add(20*time.Millisecond))
	if len(evs) != 0 {
		t.Fatalf("expected worker with cpu=2 to get no task when only cpu=4 remains, got %+v", evs)
	}

	// Worker with cpu=4 can lease heavy.
	evs = applyStep(engine.CmdLeaseNextTask{
		WorkerID:           "w-large",
		AvailableResources: map[string]int{"cpu": 4},
	}, now.Add(30*time.Millisecond))
	if len(evs) != 1 || evs[0].TaskID != "heavy" {
		t.Fatalf("expected worker with cpu=4 to lease heavy, got %+v", evs)
	}

	if _, err := engine.VerifyReplay(dag, journal); err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}

	// Backward compatibility: nil AvailableResources is unconstrained and leases highest-priority heavy.
	stateUnconstrained := engine.NewRunState(dag, "run_unconstrained")
	startEvs, _, err := engine.Step(dag, stateUnconstrained, engine.CmdStartRun{RunID: "run_unconstrained"}, now)
	if err != nil {
		t.Fatalf("start unconstrained: %v", err)
	}
	if err := engine.Apply(stateUnconstrained, startEvs[0]); err != nil {
		t.Fatalf("apply start: %v", err)
	}
	unconstrainedEvs, _, err := engine.Step(dag, stateUnconstrained, engine.CmdLeaseNextTask{WorkerID: "w-legacy"}, now.Add(10*time.Millisecond))
	if err != nil || len(unconstrainedEvs) != 1 || unconstrainedEvs[0].TaskID != "heavy" {
		t.Fatalf("expected unconstrained worker to lease heavy, got %+v err=%v", unconstrainedEvs, err)
	}
}

func TestExponentialRetryBackoff(t *testing.T) {
	t.Parallel()

	const yamlSpec = `
name: backoff-pipeline
tasks:
  - id: flaky
    handler: sim
    max_retries: 2
    retry_backoff_ms: 1000
`
	dag, err := engine.ParseDAG([]byte(yamlSpec))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	state := engine.NewRunState(dag, "run_backoff")
	var journal []engine.Event

	applyStep := func(cmd engine.Command, ts time.Time) []engine.Event {
		evs, _, err := engine.Step(dag, state, cmd, ts)
		if err != nil {
			t.Fatalf("Step(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply(%s): %v", ev.Type, err)
			}
			journal = append(journal, ev)
		}
		return evs
	}

	applyStep(engine.CmdStartRun{RunID: "run_backoff"}, now)

	// Attempt 1 leased at t=0, fails at t=100ms -> backoff = 1000ms * 2^0 = 1000ms (retry after t=1100ms).
	evs := applyStep(engine.CmdLeaseNextTask{WorkerID: "w-1"}, now)
	if len(evs) != 1 || evs[0].Attempt != 1 || evs[0].FencingToken != 1 {
		t.Fatalf("unexpected attempt 1 lease: %+v", evs)
	}
	applyStep(engine.CmdFailTask{TaskID: "flaky", WorkerID: "w-1", FencingToken: 1, ErrMsg: "boom-1"}, now.Add(100*time.Millisecond))

	wantRetryAfter1 := now.Add(1100 * time.Millisecond).UnixNano()
	if state.Tasks["flaky"].RetryAfterNs != wantRetryAfter1 {
		t.Fatalf("expected RetryAfterNs=%d, got %d", wantRetryAfter1, state.Tasks["flaky"].RetryAfterNs)
	}

	// Polling before backoff elapses (t=1099ms) must not lease the task.
	if early := applyStep(engine.CmdLeaseNextTask{WorkerID: "w-2"}, now.Add(1099*time.Millisecond)); len(early) != 0 {
		t.Fatalf("expected backoff to block early re-lease, got %+v", early)
	}

	// Polling at t=1100ms leases attempt 2.
	evs = applyStep(engine.CmdLeaseNextTask{WorkerID: "w-2"}, now.Add(1100*time.Millisecond))
	if len(evs) != 1 || evs[0].Attempt != 2 || evs[0].FencingToken != 2 {
		t.Fatalf("expected attempt 2 lease at t=1100ms, got %+v", evs)
	}

	// Attempt 2 fails at t=1200ms -> backoff = 1000ms * 2^1 = 2000ms (retry after t=3200ms).
	applyStep(engine.CmdFailTask{TaskID: "flaky", WorkerID: "w-2", FencingToken: 2, ErrMsg: "boom-2"}, now.Add(1200*time.Millisecond))
	wantRetryAfter2 := now.Add(3200 * time.Millisecond).UnixNano()
	if state.Tasks["flaky"].RetryAfterNs != wantRetryAfter2 {
		t.Fatalf("expected RetryAfterNs=%d, got %d", wantRetryAfter2, state.Tasks["flaky"].RetryAfterNs)
	}

	if early := applyStep(engine.CmdLeaseNextTask{WorkerID: "w-3"}, now.Add(3199*time.Millisecond)); len(early) != 0 {
		t.Fatalf("expected 2x backoff to block early re-lease, got %+v", early)
	}

	evs = applyStep(engine.CmdLeaseNextTask{WorkerID: "w-3"}, now.Add(3200*time.Millisecond))
	if len(evs) != 1 || evs[0].Attempt != 3 || evs[0].FencingToken != 3 {
		t.Fatalf("expected attempt 3 lease at t=3200ms, got %+v", evs)
	}

	if _, err := engine.VerifyReplay(dag, journal); err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
}

func TestTaskDeadlineEnforcement(t *testing.T) {
	t.Parallel()

	const yamlSpec = `
name: deadline-pipeline
tasks:
  - id: slow_task
    handler: sim
    max_retries: 1
    deadline_ms: 1000
`
	dag, err := engine.ParseDAG([]byte(yamlSpec))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	state := engine.NewRunState(dag, "run_deadline")
	var journal []engine.Event

	applyStep := func(cmd engine.Command, ts time.Time) []engine.Event {
		evs, _, err := engine.Step(dag, state, cmd, ts)
		if err != nil {
			t.Fatalf("Step(%T): %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply(%s): %v", ev.Type, err)
			}
			journal = append(journal, ev)
		}
		return evs
	}

	applyStep(engine.CmdStartRun{RunID: "run_deadline"}, now)
	// Lease with a long 10s TTL so only the 1000ms task deadline can trigger expiry.
	applyStep(engine.CmdLeaseNextTask{WorkerID: "w-1", LeaseTTL: 10 * time.Second}, now)
	applyStep(engine.CmdMarkTaskStarted{TaskID: "slow_task", WorkerID: "w-1", FencingToken: 1}, now.Add(100*time.Millisecond))

	// Before deadline (999ms): should not expire.
	if evs := applyStep(engine.CmdExpireLeases{}, now.Add(999*time.Millisecond)); len(evs) != 0 {
		t.Fatalf("expected no expiry before deadline, got %+v", evs)
	}

	// At deadline (1000ms): must expire with "task deadline exceeded" and TASK_OUTCOME_AMBIGUOUS.
	expiredEvs := applyStep(engine.CmdExpireLeases{}, now.Add(1000*time.Millisecond))
	if len(expiredEvs) != 2 || expiredEvs[0].Type != engine.EventLeaseExpired || expiredEvs[1].Type != engine.EventTaskOutcomeAmbiguous {
		t.Fatalf("expected LEASE_EXPIRED + TASK_OUTCOME_AMBIGUOUS, got %+v", expiredEvs)
	}
	var payload engine.LeaseExpiredPayload
	if err := json.Unmarshal(expiredEvs[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal LeaseExpiredPayload: %v", err)
	}
	if payload.Reason != "task deadline exceeded" || !payload.WillRetry {
		t.Fatalf("unexpected LeaseExpiredPayload: %+v", payload)
	}
	if state.Tasks["slow_task"].AmbiguousCount != 1 || state.Tasks["slow_task"].Status != engine.TaskStatusPending {
		t.Fatalf("unexpected task state after deadline expiry: %+v", state.Tasks["slow_task"])
	}

	rep, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if rep.AmbiguousEvents != 1 {
		t.Fatalf("expected 1 ambiguous event in replay report, got %+v", rep)
	}
}

func TestVerifyReplayRejectsDependencyViolationAndNonIncreasingFencingToken(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	startPayload, err := json.Marshal(engine.RunStartedPayload{
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
	})
	if err != nil {
		t.Fatalf("marshal start: %v", err)
	}
	leasePayload, err := json.Marshal(engine.TaskLeasedPayload{
		LeaseExpiresAtNs: now.Add(3 * time.Second).UnixNano(),
	})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}

	// Corrupted stream 1: "transform" (depends on "fetch") leased before "fetch" completed.
	depViolationEvents := []engine.Event{
		{
			RunID:       "run_corrupt_dep",
			Seq:         1,
			TimestampNs: now.UnixNano(),
			Type:        engine.EventRunStarted,
			Payload:     startPayload,
		},
		{
			RunID:        "run_corrupt_dep",
			Seq:          2,
			TimestampNs:  now.Add(10 * time.Millisecond).UnixNano(),
			Type:         engine.EventTaskLeased,
			TaskID:       "transform",
			Attempt:      1,
			FencingToken: 1,
			WorkerID:     "w-1",
			Payload:      leasePayload,
		},
	}
	if _, err := engine.VerifyReplay(dag, depViolationEvents); err == nil || !strings.Contains(err.Error(), "dependency violation") {
		t.Fatalf("expected VerifyReplay dependency violation error, got %v", err)
	}

	// Corrupted stream 2: fencing_token is not strictly increasing across TASK_LEASED events.
	completePayload, err := json.Marshal(engine.TaskCompletedPayload{Output: map[string]any{"ok": true}})
	if err != nil {
		t.Fatalf("marshal complete: %v", err)
	}
	nonIncreasingTokenEvents := []engine.Event{
		{
			RunID:       "run_corrupt_tok",
			Seq:         1,
			TimestampNs: now.UnixNano(),
			Type:        engine.EventRunStarted,
			Payload:     startPayload,
		},
		{
			RunID:        "run_corrupt_tok",
			Seq:          2,
			TimestampNs:  now.Add(10 * time.Millisecond).UnixNano(),
			Type:         engine.EventTaskLeased,
			TaskID:       "fetch",
			Attempt:      1,
			FencingToken: 2,
			WorkerID:     "w-1",
			Payload:      leasePayload,
		},
		{
			RunID:        "run_corrupt_tok",
			Seq:          3,
			TimestampNs:  now.Add(20 * time.Millisecond).UnixNano(),
			Type:         engine.EventTaskCompleted,
			TaskID:       "fetch",
			Attempt:      1,
			FencingToken: 2,
			WorkerID:     "w-1",
			Payload:      completePayload,
		},
		{
			RunID:        "run_corrupt_tok",
			Seq:          4,
			TimestampNs:  now.Add(30 * time.Millisecond).UnixNano(),
			Type:         engine.EventTaskLeased,
			TaskID:       "transform",
			Attempt:      1,
			FencingToken: 2, // non-increasing!
			WorkerID:     "w-2",
			Payload:      leasePayload,
		},
	}
	if _, err := engine.VerifyReplay(dag, nonIncreasingTokenEvents); err == nil || !strings.Contains(err.Error(), "non-increasing fencing_token") {
		t.Fatalf("expected VerifyReplay non-increasing fencing_token error, got %v", err)
	}
}
