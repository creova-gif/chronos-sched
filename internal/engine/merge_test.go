package engine

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const mergeTestWorkflowYAML = `
name: merge-pipeline
tasks:
  - id: ingest
    handler: echo
    checkpoint_name: after_ingest
  - id: branch_left
    handler: echo
    depends_on: [ingest]
    priority: 10
  - id: branch_right
    handler: echo
    depends_on: [ingest]
    priority: 5
  - id: join
    handler: echo
    depends_on: [branch_left, branch_right]
  - id: publish
    handler: echo
    depends_on: [join]
`

func mergeTestParseDAG(t *testing.T) *DAG {
	t.Helper()
	dag, err := ParseDAG([]byte(mergeTestWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}
	return dag
}

func mergeTestMustStep(t *testing.T, dag *DAG, events *[]Event, state *RunState, cmd Command, now time.Time) {
	t.Helper()
	newEvs, _, err := Step(dag, state, cmd, now)
	if err != nil {
		t.Fatalf("Step(%T): %v", cmd, err)
	}
	for _, ev := range newEvs {
		if err := Apply(state, ev); err != nil {
			t.Fatalf("Apply(%s): %v", ev.Type, err)
		}
		*events = append(*events, ev)
	}
}

func mergeTestCompleteTask(t *testing.T, dag *DAG, events *[]Event, state *RunState, workerID string, expectedTaskID string, output map[string]any, now time.Time) {
	t.Helper()
	mergeTestMustStep(t, dag, events, state, CmdLeaseNextTask{WorkerID: workerID}, now)
	leased := (*events)[len(*events)-1]
	if leased.Type != EventTaskLeased || leased.TaskID != expectedTaskID {
		t.Fatalf("expected lease for %q, got type=%s task=%q", expectedTaskID, leased.Type, leased.TaskID)
	}
	mergeTestMustStep(t, dag, events, state, CmdMarkTaskStarted{
		TaskID:       leased.TaskID,
		WorkerID:     workerID,
		FencingToken: leased.FencingToken,
	}, now.Add(10*time.Millisecond))
	mergeTestMustStep(t, dag, events, state, CmdCompleteTask{
		TaskID:       leased.TaskID,
		WorkerID:     workerID,
		FencingToken: leased.FencingToken,
		Output:       output,
	}, now.Add(20*time.Millisecond))
}

func mergeTestBuildSharedPrefix(t *testing.T, dag *DAG, baseTime time.Time) ([]Event, *RunState) {
	t.Helper()
	state := NewRunState(dag, "run-base")
	var events []Event
	mergeTestMustStep(t, dag, &events, state, CmdStartRun{RunID: "run-base"}, baseTime)
	mergeTestCompleteTask(t, dag, &events, state, "w1", "ingest", map[string]any{"rows": float64(100)}, baseTime.Add(time.Second))
	return events, state
}

func mergeTestForkFromPrefix(t *testing.T, dag *DAG, prefix []Event, branchRunID string, now time.Time) ([]Event, *RunState) {
	t.Helper()
	prefixState, err := Fold(dag, prefix)
	if err != nil {
		t.Fatalf("Fold prefix: %v", err)
	}
	forkEvs, _, err := Step(dag, prefixState, CmdForkRun{
		NewRunID:           branchRunID,
		CheckpointSelector: "after_ingest",
	}, now)
	if err != nil {
		t.Fatalf("Step(CmdForkRun): %v", err)
	}
	branchEvents := append([]Event(nil), prefix...)
	branchEvents = append(branchEvents, forkEvs...)
	branchState, err := Fold(dag, branchEvents)
	if err != nil {
		t.Fatalf("Fold branch: %v", err)
	}
	return branchEvents, branchState
}

func TestMergeCleanDisjointAdoptsAndInvalidatesTransitiveDownstream(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	baseEvents, baseState := mergeTestBuildSharedPrefix(t, dag, t0)
	prefixLen := len(baseEvents)

	// Fork branch right after ingest checkpoint.
	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents[:prefixLen], "run-branch", t0.Add(2*time.Second))

	// Base advances branch_left only; branch_right remains pending on base.
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_left", map[string]any{"left": "v1"}, t0.Add(3*time.Second))

	// Branch leases branch_left (leaves it running) and completes branch_right.
	mergeTestMustStep(t, dag, &branchEvents, branchState, CmdLeaseNextTask{WorkerID: "w2"}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w3", "branch_right", map[string]any{"right": "v2"}, t0.Add(4*time.Second))

	plan, err := PlanBranchMerge(dag, baseEvents, branchEvents, nil, MergeFailOnConflict)
	if err != nil {
		t.Fatalf("PlanBranchMerge unexpected error: %v", err)
	}

	if plan.BaseRunID != "run-base" || plan.BranchRunID != "run-branch" {
		t.Fatalf("unexpected run IDs: base=%q branch=%q", plan.BaseRunID, plan.BranchRunID)
	}
	if plan.CommonPrefixSeq != int64(prefixLen) {
		t.Fatalf("CommonPrefixSeq = %d, want %d", plan.CommonPrefixSeq, prefixLen)
	}
	if len(plan.Conflicts) != 0 {
		t.Fatalf("expected 0 conflicts, got %+v", plan.Conflicts)
	}
	if len(plan.AdoptedTasks) != 1 || plan.AdoptedTasks[0].TaskID != "branch_right" {
		t.Fatalf("AdoptedTasks = %+v, want [branch_right]", plan.AdoptedTasks)
	}
	wantOutput := map[string]any{"right": "v2"}
	if !reflect.DeepEqual(plan.AdoptedTasks[0].Output, wantOutput) {
		t.Fatalf("adopted output = %#v, want %#v", plan.AdoptedTasks[0].Output, wantOutput)
	}
	// Transitive downstream of branch_right in topological order: join -> publish.
	wantInvalidated := []string{"join", "publish"}
	if !reflect.DeepEqual(plan.InvalidatedDownstream, wantInvalidated) {
		t.Fatalf("InvalidatedDownstream = %v, want %v", plan.InvalidatedDownstream, wantInvalidated)
	}
}

func TestMergeConflictFailAndPreferBranch(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	baseEvents, baseState := mergeTestBuildSharedPrefix(t, dag, t0)
	prefixLen := len(baseEvents)

	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents[:prefixLen], "run-branch", t0.Add(2*time.Second))

	// Both base and branch complete branch_left with different outputs after the fork.
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_left", map[string]any{"score": float64(0.81)}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_left", map[string]any{"score": float64(0.95)}, t0.Add(3*time.Second))

	// 1. MergeFailOnConflict must return ErrMergeConflict AND the populated plan.
	planFail, err := PlanBranchMerge(dag, baseEvents, branchEvents, nil, MergeFailOnConflict)
	if !errors.Is(err, ErrMergeConflict) {
		t.Fatalf("expected ErrMergeConflict, got %v", err)
	}
	if planFail == nil {
		t.Fatal("expected non-nil MergePlan alongside ErrMergeConflict")
	}
	if len(planFail.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(planFail.Conflicts))
	}
	c := planFail.Conflicts[0]
	if c.TaskID != "branch_left" {
		t.Fatalf("conflict task = %q, want branch_left", c.TaskID)
	}
	if !reflect.DeepEqual(c.BaseOutput, map[string]any{"score": float64(0.81)}) {
		t.Fatalf("conflict BaseOutput = %#v", c.BaseOutput)
	}
	if !reflect.DeepEqual(c.BranchOutput, map[string]any{"score": float64(0.95)}) {
		t.Fatalf("conflict BranchOutput = %#v", c.BranchOutput)
	}
	if len(planFail.AdoptedTasks) != 0 {
		t.Fatalf("expected 0 adopted tasks on fail_on_conflict, got %+v", planFail.AdoptedTasks)
	}

	// 2. MergePreferBranch resolves the conflict by adopting branch's output and invalidating downstream.
	planPrefer, err := PlanBranchMerge(dag, baseEvents, branchEvents, nil, MergePreferBranch)
	if err != nil {
		t.Fatalf("PlanBranchMerge(MergePreferBranch) unexpected error: %v", err)
	}
	if len(planPrefer.Conflicts) != 1 || planPrefer.Conflicts[0].TaskID != "branch_left" {
		t.Fatalf("expected conflict metadata preserved under MergePreferBranch, got %+v", planPrefer.Conflicts)
	}
	if len(planPrefer.AdoptedTasks) != 1 || planPrefer.AdoptedTasks[0].TaskID != "branch_left" {
		t.Fatalf("expected branch_left adopted under MergePreferBranch, got %+v", planPrefer.AdoptedTasks)
	}
	if !reflect.DeepEqual(planPrefer.AdoptedTasks[0].Output, map[string]any{"score": float64(0.95)}) {
		t.Fatalf("adopted output = %#v, want score=0.95", planPrefer.AdoptedTasks[0].Output)
	}
	wantInvalidated := []string{"join", "publish"}
	if !reflect.DeepEqual(planPrefer.InvalidatedDownstream, wantInvalidated) {
		t.Fatalf("InvalidatedDownstream = %v, want %v", planPrefer.InvalidatedDownstream, wantInvalidated)
	}
}

func TestMergeCherryPickValidation(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	baseEvents, _ := mergeTestBuildSharedPrefix(t, dag, t0)
	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents, "run-branch", t0.Add(2*time.Second))

	// Complete branch_left on branch, leave branch_right pending.
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_left", map[string]any{"ok": true}, t0.Add(3*time.Second))

	// Reject unknown task ID.
	if _, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"does_not_exist"}, MergeFailOnConflict); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound for unknown task, got %v", err)
	}

	// Reject incomplete task on branch.
	if _, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"branch_right"}, MergeFailOnConflict); err == nil || !strings.Contains(err.Error(), "cannot cherry-pick") {
		t.Fatalf("expected cherry-pick status error for incomplete task, got %v", err)
	}

	// Succeed for explicit completed task and exclude adopted tasks from InvalidatedDownstream.
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_right", map[string]any{"ok": true}, t0.Add(4*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "join", map[string]any{"joined": true}, t0.Add(5*time.Second))

	plan, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"join", "branch_left"}, MergeFailOnConflict)
	if err != nil {
		t.Fatalf("cherry-pick unexpected error: %v", err)
	}
	// AdoptedTasks must be ordered topologically (branch_left before join), and join must not appear in InvalidatedDownstream.
	if len(plan.AdoptedTasks) != 2 || plan.AdoptedTasks[0].TaskID != "branch_left" || plan.AdoptedTasks[1].TaskID != "join" {
		t.Fatalf("AdoptedTasks = %+v, want [branch_left, join]", plan.AdoptedTasks)
	}
	if !reflect.DeepEqual(plan.InvalidatedDownstream, []string{"publish"}) {
		t.Fatalf("InvalidatedDownstream = %v, want [publish]", plan.InvalidatedDownstream)
	}
}

func TestMergeFormatMergePlan(t *testing.T) {
	plan := &MergePlan{
		BaseRunID:       "run-base",
		BranchRunID:     "run-branch",
		CommonPrefixSeq: 6,
		Strategy:        MergePreferBranch,
		AdoptedTasks: []AdoptedTask{
			{
				TaskID:             "branch_left",
				BranchCompletedSeq: 10,
				Output:             map[string]any{"score": 0.95},
			},
		},
		Conflicts: []MergeConflict{
			{
				TaskID:             "branch_left",
				BaseStatus:         TaskStatusCompleted,
				BranchStatus:       TaskStatusCompleted,
				BaseCompletedSeq:   9,
				BranchCompletedSeq: 10,
				BaseOutput:         map[string]any{"score": 0.81},
				BranchOutput:       map[string]any{"score": 0.95},
			},
		},
		InvalidatedDownstream: []string{"join", "publish"},
	}

	formatted := FormatMergePlan(plan)
	wantSubstrings := []string{
		"Timeline Merge Plan: run-base <- run-branch",
		"Common prefix through seq: 6",
		"Strategy: prefer_branch",
		"Adopted tasks (1):",
		"+ branch_left (branch_seq=10, output={\"score\":0.95})",
		"Conflicts (1):",
		"! branch_left: base(seq=9, output={\"score\":0.81}) vs branch(seq=10, output={\"score\":0.95})",
		"Invalidated downstream (2):",
		"~ join (re-execute)",
		"~ publish (re-execute)",
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(formatted, sub) {
			t.Errorf("FormatMergePlan output missing %q\nFull output:\n%s", sub, formatted)
		}
	}
}
