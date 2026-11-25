package engine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func applyStep(t *testing.T, dag *engine.DAG, state *engine.RunState, journal *[]engine.Event, cmd engine.Command, now time.Time) ([]engine.Event, []engine.Effect) {
	t.Helper()
	evs, effs, err := engine.Step(dag, state, cmd, now)
	if err != nil {
		t.Fatalf("Step(%T) failed: %v", cmd, err)
	}
	for _, ev := range evs {
		if err := engine.Apply(state, ev); err != nil {
			t.Fatalf("Apply(%s) failed: %v", ev.Type, err)
		}
		*journal = append(*journal, ev)
	}
	return evs, effs
}

func TestLifecyclePauseDrainAndResume(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	state := engine.NewRunState(dag, "run-lifecycle-1")
	var journal []engine.Event

	// Cannot pause or resume before start.
	if _, _, err := engine.Step(dag, state, engine.CmdPauseRun{Reason: "too early"}, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition pausing PENDING run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdResumeRun{Reason: "too early"}, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition resuming PENDING run, got %v", err)
	}

	// Start run and complete fetch so transform and enrich become ready.
	applyStep(t, dag, state, &journal, engine.CmdStartRun{RunID: "run-lifecycle-1"}, now)
	now = now.Add(time.Second)

	_, effs := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second}, now)
	if len(effs) != 1 || effs[0].TaskID != "fetch" {
		t.Fatalf("expected fetch leased, got %+v", effs)
	}
	applyStep(t, dag, state, &journal, engine.CmdMarkTaskStarted{TaskID: "fetch", WorkerID: "w1", FencingToken: effs[0].FencingToken}, now)
	applyStep(t, dag, state, &journal, engine.CmdCompleteTask{TaskID: "fetch", WorkerID: "w1", FencingToken: effs[0].FencingToken, Output: map[string]any{"rows": 42}}, now)

	// Lease enrich (leaves transform pending), then pause while enrich is in-flight.
	now = now.Add(time.Second)
	_, effs = applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second}, now)
	if len(effs) != 1 || effs[0].TaskID != "enrich" {
		t.Fatalf("expected enrich leased, got %+v", effs)
	}
	inFlightToken := effs[0].FencingToken

	now = now.Add(time.Second)
	evs, pauseEffs := applyStep(t, dag, state, &journal, engine.CmdPauseRun{Reason: "maintenance window"}, now)
	if len(evs) != 1 || evs[0].Type != engine.EventRunPaused || len(pauseEffs) != 0 {
		t.Fatalf("unexpected pause output: evs=%+v effs=%+v", evs, pauseEffs)
	}
	if state.Status != engine.RunStatusPaused {
		t.Fatalf("expected status PAUSED, got %s", state.Status)
	}

	// Double-pause is rejected.
	if _, _, err := engine.Step(dag, state, engine.CmdPauseRun{Reason: "again"}, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition on double pause, got %v", err)
	}

	// Paused run must lease zero new work (transform remains PENDING).
	leaseEvs, leaseEffs, err := engine.Step(dag, state, engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 5 * time.Second}, now)
	if err != nil || len(leaseEvs) != 0 || len(leaseEffs) != 0 {
		t.Fatalf("expected zero leases while paused, got evs=%d effs=%d err=%v", len(leaseEvs), len(leaseEffs), err)
	}
	if state.Tasks["transform"].Status != engine.TaskStatusPending {
		t.Fatalf("expected transform to stay PENDING while paused, got %s", state.Tasks["transform"].Status)
	}

	// In-flight task enrich drains cleanly (mark started + complete) while paused.
	now = now.Add(time.Second)
	applyStep(t, dag, state, &journal, engine.CmdMarkTaskStarted{TaskID: "enrich", WorkerID: "w1", FencingToken: inFlightToken}, now)
	applyStep(t, dag, state, &journal, engine.CmdCompleteTask{TaskID: "enrich", WorkerID: "w1", FencingToken: inFlightToken, Output: map[string]any{"ok": true}}, now)
	if state.Tasks["enrich"].Status != engine.TaskStatusCompleted {
		t.Fatalf("expected enrich COMPLETED while paused, got %s", state.Tasks["enrich"].Status)
	}
	if state.Status != engine.RunStatusPaused {
		t.Fatalf("expected run to remain PAUSED after non-final task completion, got %s", state.Status)
	}

	// Still no new leases while paused.
	leaseEvs, _, err = engine.Step(dag, state, engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 5 * time.Second}, now)
	if err != nil || len(leaseEvs) != 0 {
		t.Fatalf("expected zero leases while paused after drain, got evs=%d err=%v", len(leaseEvs), err)
	}

	// Resume re-enables leasing and finishes remaining DAG.
	now = now.Add(time.Second)
	evs, _ = applyStep(t, dag, state, &journal, engine.CmdResumeRun{Reason: "maintenance done"}, now)
	if len(evs) != 1 || evs[0].Type != engine.EventRunResumed {
		t.Fatalf("expected RUN_RESUMED, got %+v", evs)
	}
	if state.Status != engine.RunStatusRunning {
		t.Fatalf("expected status RUNNING after resume, got %s", state.Status)
	}

	// Double-resume is rejected.
	if _, _, err := engine.Step(dag, state, engine.CmdResumeRun{Reason: "again"}, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition resuming RUNNING run, got %v", err)
	}

	// Finish transform and aggregate.
	for _, expectedTask := range []string{"transform", "aggregate"} {
		now = now.Add(time.Second)
		_, effs = applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 5 * time.Second}, now)
		if len(effs) != 1 || effs[0].TaskID != expectedTask {
			t.Fatalf("expected %s leased after resume, got %+v", expectedTask, effs)
		}
		applyStep(t, dag, state, &journal, engine.CmdCompleteTask{TaskID: expectedTask, WorkerID: "w2", FencingToken: effs[0].FencingToken}, now)
	}

	if state.Status != engine.RunStatusCompleted {
		t.Fatalf("expected final status COMPLETED, got %s", state.Status)
	}

	folded, err := engine.Fold(dag, journal)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	if folded.Status != engine.RunStatusCompleted || folded.LastSeq != state.LastSeq {
		t.Fatalf("folded state mismatch: got status=%s seq=%d, want COMPLETED seq=%d", folded.Status, folded.LastSeq, state.LastSeq)
	}

	report, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if report.FinalStatus != engine.RunStatusCompleted || report.CompletedTasks != 4 {
		t.Fatalf("unexpected replay report: %+v", report)
	}
}

func TestLifecycleLastInFlightTaskCompletesWhilePaused(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(`
name: single-task
tasks:
  - id: only
    handler: sim
`))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	state := engine.NewRunState(dag, "run-pause-finish")
	var journal []engine.Event

	applyStep(t, dag, state, &journal, engine.CmdStartRun{RunID: "run-pause-finish"}, now)
	_, effs := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second}, now)
	token := effs[0].FencingToken

	applyStep(t, dag, state, &journal, engine.CmdPauseRun{Reason: "drain only"}, now)
	if state.Status != engine.RunStatusPaused {
		t.Fatalf("expected PAUSED, got %s", state.Status)
	}

	evs, completeEffs := applyStep(t, dag, state, &journal, engine.CmdCompleteTask{TaskID: "only", WorkerID: "w1", FencingToken: token}, now)
	if state.Status != engine.RunStatusCompleted {
		t.Fatalf("expected COMPLETED after last in-flight task finishes while paused, got %s", state.Status)
	}
	foundRunCompleted := false
	for _, ev := range evs {
		if ev.Type == engine.EventRunCompleted {
			foundRunCompleted = true
		}
	}
	if !foundRunCompleted {
		t.Fatalf("expected EventRunCompleted in emitted events, got %+v", evs)
	}
	if len(completeEffs) != 1 || completeEffs[0].Kind != engine.EffectRunTerminal || completeEffs[0].Status != engine.RunStatusCompleted {
		t.Fatalf("expected EffectRunTerminal(COMPLETED), got %+v", completeEffs)
	}

	report, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if report.FinalStatus != engine.RunStatusCompleted || report.CompletedTasks != 1 {
		t.Fatalf("unexpected replay report: %+v", report)
	}
}

func TestLifecyclePausedFailAndExpireLeases(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(`
name: two-parallel
tasks:
  - id: a
    handler: sim
    max_retries: 1
  - id: b
    handler: sim
    max_retries: 1
`))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	state := engine.NewRunState(dag, "run-pause-drain-fail")
	var journal []engine.Event

	applyStep(t, dag, state, &journal, engine.CmdStartRun{RunID: "run-pause-drain-fail"}, now)
	_, effA := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 2 * time.Second}, now)
	_, effB := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 2 * time.Second}, now)

	applyStep(t, dag, state, &journal, engine.CmdPauseRun{Reason: "draining"}, now)

	// Fail task a while paused (retries back to PENDING).
	applyStep(t, dag, state, &journal, engine.CmdFailTask{
		TaskID:       effA[0].TaskID,
		WorkerID:     "w1",
		FencingToken: effA[0].FencingToken,
		ErrMsg:       "transient error",
	}, now)
	if state.Tasks[effA[0].TaskID].Status != engine.TaskStatusPending {
		t.Fatalf("expected task %s PENDING after retryable fail while paused, got %s", effA[0].TaskID, state.Tasks[effA[0].TaskID].Status)
	}

	// Expire lease on task b while paused.
	now = now.Add(3 * time.Second)
	evs, _ := applyStep(t, dag, state, &journal, engine.CmdExpireLeases{}, now)
	if len(evs) == 0 || evs[0].Type != engine.EventLeaseExpired {
		t.Fatalf("expected LEASE_EXPIRED while paused, got %+v", evs)
	}
	if state.Tasks[effB[0].TaskID].Status != engine.TaskStatusPending {
		t.Fatalf("expected task %s PENDING after lease expiry while paused, got %s", effB[0].TaskID, state.Tasks[effB[0].TaskID].Status)
	}

	// Neither task is re-leased while paused.
	leaseEvs, _, err := engine.Step(dag, state, engine.CmdLeaseNextTask{WorkerID: "w3", LeaseTTL: 2 * time.Second}, now)
	if err != nil || len(leaseEvs) != 0 {
		t.Fatalf("expected no leases while paused, got evs=%d err=%v", len(leaseEvs), err)
	}
}

func TestLifecycleCancelAndReplay(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	state := engine.NewRunState(dag, "run-cancel-1")
	var journal []engine.Event

	applyStep(t, dag, state, &journal, engine.CmdStartRun{RunID: "run-cancel-1"}, now)
	_, effFetch := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second}, now)
	applyStep(t, dag, state, &journal, engine.CmdCompleteTask{TaskID: "fetch", WorkerID: "w1", FencingToken: effFetch[0].FencingToken}, now)

	// Lease first parallel task (mark running) and second parallel task (leave leased).
	_, effFirst := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w1", LeaseTTL: 5 * time.Second}, now)
	applyStep(t, dag, state, &journal, engine.CmdMarkTaskStarted{TaskID: effFirst[0].TaskID, WorkerID: "w1", FencingToken: effFirst[0].FencingToken}, now)
	_, effSecond := applyStep(t, dag, state, &journal, engine.CmdLeaseNextTask{WorkerID: "w2", LeaseTTL: 5 * time.Second}, now)

	// Pause, then cancel from PAUSED.
	now = now.Add(time.Second)
	applyStep(t, dag, state, &journal, engine.CmdPauseRun{Reason: "investigating"}, now)

	now = now.Add(time.Second)
	cancelEvs, cancelEffs := applyStep(t, dag, state, &journal, engine.CmdCancelRun{Reason: "operator cancelled bad deploy"}, now)
	if len(cancelEvs) != 1 || cancelEvs[0].Type != engine.EventRunCancelled {
		t.Fatalf("expected EventRunCancelled, got %+v", cancelEvs)
	}
	if len(cancelEffs) != 1 || cancelEffs[0].Kind != engine.EffectRunTerminal || cancelEffs[0].Status != engine.RunStatusCancelled {
		t.Fatalf("expected EffectRunTerminal(CANCELLED), got %+v", cancelEffs)
	}
	if state.Status != engine.RunStatusCancelled {
		t.Fatalf("expected status CANCELLED, got %s", state.Status)
	}
	if state.FailureReason != "operator cancelled bad deploy" {
		t.Fatalf("unexpected failure reason: %q", state.FailureReason)
	}

	// Completed task remains intact; leased/running tasks have leases cleared back to PENDING.
	if state.Tasks["fetch"].Status != engine.TaskStatusCompleted {
		t.Fatalf("expected fetch to remain COMPLETED, got %s", state.Tasks["fetch"].Status)
	}
	for _, id := range []string{"transform", "enrich"} {
		ts := state.Tasks[id]
		if ts.Status != engine.TaskStatusPending || ts.WorkerID != "" || ts.LeaseExpiresAtNs != 0 || ts.StartedAtNs != 0 {
			t.Fatalf("expected task %s lease cleared back to PENDING, got %+v", id, ts)
		}
	}

	// Subsequent mutations on terminal CANCELLED run are rejected.
	if _, _, err := engine.Step(dag, state, engine.CmdPauseRun{Reason: "nope"}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal pausing CANCELLED run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdResumeRun{Reason: "nope"}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal resuming CANCELLED run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdCancelRun{Reason: "nope"}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal cancelling CANCELLED run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdCompleteTask{TaskID: effFirst[0].TaskID, WorkerID: "w1", FencingToken: effFirst[0].FencingToken}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal completing task on CANCELLED run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdMarkTaskStarted{TaskID: effSecond[0].TaskID, WorkerID: "w2", FencingToken: effSecond[0].FencingToken}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal starting task on CANCELLED run, got %v", err)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdFailTask{TaskID: effFirst[0].TaskID, WorkerID: "w1", FencingToken: effFirst[0].FencingToken, ErrMsg: "late"}, now); !errors.Is(err, engine.ErrRunTerminal) {
		t.Fatalf("expected ErrRunTerminal failing task on CANCELLED run, got %v", err)
	}
	if evs, _, err := engine.Step(dag, state, engine.CmdLeaseNextTask{WorkerID: "w3", LeaseTTL: 5 * time.Second}, now); err != nil || len(evs) != 0 {
		t.Fatalf("expected zero leases on CANCELLED run, got evs=%d err=%v", len(evs), err)
	}

	// Deterministic Fold + VerifyReplay.
	folded, err := engine.Fold(dag, journal)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	if folded.Status != engine.RunStatusCancelled || folded.FailureReason != "operator cancelled bad deploy" {
		t.Fatalf("folded cancelled state mismatch: status=%s reason=%q", folded.Status, folded.FailureReason)
	}
	if folded.Tasks["fetch"].Status != engine.TaskStatusCompleted || folded.Tasks["transform"].Status != engine.TaskStatusPending || folded.Tasks["enrich"].Status != engine.TaskStatusPending {
		t.Fatalf("folded task states mismatch: fetch=%s transform=%s enrich=%s",
			folded.Tasks["fetch"].Status, folded.Tasks["transform"].Status, folded.Tasks["enrich"].Status)
	}

	report, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if report.FinalStatus != engine.RunStatusCancelled || report.CompletedTasks != 1 || report.EventsReplayed != len(journal) {
		t.Fatalf("unexpected replay report: %+v", report)
	}
}

func TestLifecycleCancelFromPending(t *testing.T) {
	t.Parallel()

	dag, err := engine.ParseDAG([]byte(sampleWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	state := engine.NewRunState(dag, "run-cancel-pending")
	var journal []engine.Event

	evs, effs := applyStep(t, dag, state, &journal, engine.CmdCancelRun{Reason: "aborted before start"}, now)
	if len(evs) != 1 || evs[0].Type != engine.EventRunCancelled || evs[0].Seq != 1 {
		t.Fatalf("expected RUN_CANCELLED at seq 1, got %+v", evs)
	}
	if len(effs) != 1 || effs[0].Kind != engine.EffectRunTerminal || effs[0].Status != engine.RunStatusCancelled {
		t.Fatalf("expected EffectRunTerminal(CANCELLED), got %+v", effs)
	}
	if _, _, err := engine.Step(dag, state, engine.CmdStartRun{RunID: "run-cancel-pending"}, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition starting CANCELLED run, got %v", err)
	}

	report, err := engine.VerifyReplay(dag, journal)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if report.FinalStatus != engine.RunStatusCancelled || report.State.FailureReason != "aborted before start" {
		t.Fatalf("unexpected replay report: %+v", report)
	}
}
