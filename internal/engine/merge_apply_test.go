package engine

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMergeApplyAdoptsInvalidatesAndCompletesCleanly(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 14, 0, 0, 0, time.UTC)

	// 1. Build shared prefix (ingest) and fork branch right after ingest.
	baseEvents, baseState := mergeTestBuildSharedPrefix(t, dag, t0)
	prefixLen := len(baseEvents)
	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents[:prefixLen], "run-branch", t0.Add(2*time.Second))

	// 2. Complete the entire base run.
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_left", map[string]any{"score": float64(0.81)}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_right", map[string]any{"right": "base-v1"}, t0.Add(4*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "join", map[string]any{"combined": "base"}, t0.Add(5*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "publish", map[string]any{"published": "base"}, t0.Add(6*time.Second))
	if baseState.Status != RunStatusCompleted {
		t.Fatalf("base run status = %s, want %s", baseState.Status, RunStatusCompleted)
	}

	// 3. On branch, produce an improved branch_left output.
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_left", map[string]any{"score": float64(0.95)}, t0.Add(3*time.Second))

	// Snapshot base & branch states before merge to prove neither is mutated.
	baseLenBefore := len(baseEvents)
	branchLenBefore := len(branchEvents)
	baseLastSeq := baseState.LastSeq

	// 4. Plan and execute the merge via CmdMergeBranch.
	plan, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"branch_left"}, MergePreferBranch)
	if err != nil {
		t.Fatalf("PlanBranchMerge: %v", err)
	}

	mergeEvs, mergeEffs, err := Step(dag, baseState, CmdMergeBranch{
		NewRunID: "run-merged",
		Plan:     plan,
	}, t0.Add(7*time.Second))
	if err != nil {
		t.Fatalf("Step(CmdMergeBranch): %v", err)
	}
	if len(mergeEvs) != 1 || mergeEvs[0].Type != EventTimelineMerged {
		t.Fatalf("expected 1 TIMELINE_MERGED event, got %+v", mergeEvs)
	}
	if len(mergeEffs) != 0 {
		t.Fatalf("expected no terminal effects while downstream tasks are invalidated, got %+v", mergeEffs)
	}

	// Build the merged run's event stream from a copy of baseEvents + mergeEvs.
	mergedEvents := append([]Event(nil), baseEvents...)
	mergedEvents = append(mergedEvents, mergeEvs...)

	mergedState, err := Fold(dag, mergedEvents)
	if err != nil {
		t.Fatalf("Fold(mergedEvents): %v", err)
	}
	report, err := VerifyReplay(dag, mergedEvents)
	if err != nil {
		t.Fatalf("VerifyReplay(mergedEvents): %v", err)
	}

	if mergedState.RunID != "run-merged" {
		t.Fatalf("merged RunID = %q, want run-merged", mergedState.RunID)
	}
	if mergedState.ParentRunID != "run-base" {
		t.Fatalf("merged ParentRunID = %q, want run-base", mergedState.ParentRunID)
	}
	if mergedState.ForkFromSeq != baseLastSeq {
		t.Fatalf("merged ForkFromSeq = %d, want %d", mergedState.ForkFromSeq, baseLastSeq)
	}
	if mergedState.Status != RunStatusRunning || report.FinalStatus != RunStatusRunning {
		t.Fatalf("merged status = %s (replay=%s), want %s", mergedState.Status, report.FinalStatus, RunStatusRunning)
	}

	// Untouched base tasks remain COMPLETED with their original outputs.
	if mergedState.Tasks["ingest"].Status != TaskStatusCompleted || !reflect.DeepEqual(mergedState.Tasks["ingest"].Output, map[string]any{"rows": float64(100)}) {
		t.Fatalf("unexpected ingest state after merge: %+v", mergedState.Tasks["ingest"])
	}
	if mergedState.Tasks["branch_right"].Status != TaskStatusCompleted || !reflect.DeepEqual(mergedState.Tasks["branch_right"].Output, map[string]any{"right": "base-v1"}) {
		t.Fatalf("unexpected branch_right state after merge: %+v", mergedState.Tasks["branch_right"])
	}

	// Adopted branch_left task has branch output and CompletedSeq == merge event seq.
	adoptedTS := mergedState.Tasks["branch_left"]
	if adoptedTS.Status != TaskStatusCompleted {
		t.Fatalf("branch_left status = %s, want COMPLETED", adoptedTS.Status)
	}
	if !reflect.DeepEqual(adoptedTS.Output, map[string]any{"score": float64(0.95)}) {
		t.Fatalf("branch_left output = %#v, want score=0.95", adoptedTS.Output)
	}
	if adoptedTS.CompletedSeq != mergeEvs[0].Seq {
		t.Fatalf("branch_left CompletedSeq = %d, want %d", adoptedTS.CompletedSeq, mergeEvs[0].Seq)
	}

	// Transitive downstream tasks (join, publish) are invalidated to PENDING with cleared fields.
	for _, invID := range []string{"join", "publish"} {
		ts := mergedState.Tasks[invID]
		if ts.Status != TaskStatusPending {
			t.Fatalf("invalidated task %q status = %s, want PENDING", invID, ts.Status)
		}
		if ts.Output != nil || ts.WorkerID != "" || ts.LeaseExpiresAtNs != 0 || ts.LastError != "" || ts.CompletedSeq != 0 || ts.StartedAtNs != 0 || ts.RetryAfterNs != 0 {
			t.Fatalf("invalidated task %q did not clear state cleanly: %+v", invID, ts)
		}
	}

	// 5. Re-execute invalidated downstream tasks to complete the merged run.
	mergeTestCompleteTask(t, dag, &mergedEvents, mergedState, "w3", "join", map[string]any{"combined": "merged-0.95"}, t0.Add(8*time.Second))
	mergeTestCompleteTask(t, dag, &mergedEvents, mergedState, "w3", "publish", map[string]any{"published": "merged-0.95"}, t0.Add(9*time.Second))

	if mergedState.Status != RunStatusCompleted {
		t.Fatalf("merged run final status = %s, want %s", mergedState.Status, RunStatusCompleted)
	}

	finalReport, err := VerifyReplay(dag, mergedEvents)
	if err != nil {
		t.Fatalf("VerifyReplay(final mergedEvents): %v", err)
	}
	if finalReport.FinalStatus != RunStatusCompleted || finalReport.CompletedTasks != finalReport.TotalTasks {
		t.Fatalf("unexpected final replay report: %+v", finalReport)
	}
	if !reflect.DeepEqual(finalReport.State.Tasks["publish"].Output, map[string]any{"published": "merged-0.95"}) {
		t.Fatalf("final publish output = %#v, want merged-0.95", finalReport.State.Tasks["publish"].Output)
	}

	// 6. Verify base and branch runs were never mutated.
	if len(baseEvents) != baseLenBefore || len(branchEvents) != branchLenBefore {
		t.Fatalf("base/branch event lengths changed: base=%d (want %d), branch=%d (want %d)", len(baseEvents), baseLenBefore, len(branchEvents), branchLenBefore)
	}
	if baseState.RunID != "run-base" || !reflect.DeepEqual(baseState.Tasks["branch_left"].Output, map[string]any{"score": float64(0.81)}) {
		t.Fatalf("baseState was mutated: %+v", baseState.Tasks["branch_left"])
	}
	if branchState.RunID != "run-branch" || branchState.Tasks["join"].Status != TaskStatusPending {
		t.Fatalf("branchState was mutated: %+v", branchState.Tasks["join"])
	}
}

func TestMergeApplyImmediateCompletionOnLeafAdoption(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)

	// Build and complete base run.
	baseEvents, baseState := mergeTestBuildSharedPrefix(t, dag, t0)
	prefixLen := len(baseEvents)
	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents[:prefixLen], "run-branch", t0.Add(2*time.Second))

	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_left", map[string]any{"v": "same"}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "branch_right", map[string]any{"v": "same"}, t0.Add(4*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "join", map[string]any{"v": "same"}, t0.Add(5*time.Second))
	mergeTestCompleteTask(t, dag, &baseEvents, baseState, "w1", "publish", map[string]any{"v": "base-pub"}, t0.Add(6*time.Second))

	// Complete branch with a different leaf (publish) output.
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_left", map[string]any{"v": "same"}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "branch_right", map[string]any{"v": "same"}, t0.Add(4*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "join", map[string]any{"v": "same"}, t0.Add(5*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w2", "publish", map[string]any{"v": "branch-pub"}, t0.Add(6*time.Second))

	plan, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"publish"}, MergePreferBranch)
	if err != nil {
		t.Fatalf("PlanBranchMerge: %v", err)
	}
	if len(plan.InvalidatedDownstream) != 0 {
		t.Fatalf("expected 0 invalidated downstream tasks for leaf adoption, got %v", plan.InvalidatedDownstream)
	}

	mergeEvs, mergeEffs, err := Step(dag, baseState, CmdMergeBranch{
		NewRunID: "run-merged-leaf",
		Plan:     plan,
	}, t0.Add(7*time.Second))
	if err != nil {
		t.Fatalf("Step(CmdMergeBranch): %v", err)
	}
	if len(mergeEvs) != 2 || mergeEvs[0].Type != EventTimelineMerged || mergeEvs[1].Type != EventRunCompleted {
		t.Fatalf("expected [TIMELINE_MERGED, RUN_COMPLETED], got %+v", mergeEvs)
	}
	if mergeEvs[1].Seq != baseState.LastSeq+2 {
		t.Fatalf("RUN_COMPLETED seq = %d, want %d", mergeEvs[1].Seq, baseState.LastSeq+2)
	}
	if len(mergeEffs) != 1 || mergeEffs[0].Kind != EffectRunTerminal || mergeEffs[0].Status != RunStatusCompleted || mergeEffs[0].RunID != "run-merged-leaf" {
		t.Fatalf("expected EffectRunTerminal(COMPLETED), got %+v", mergeEffs)
	}

	mergedEvents := append([]Event(nil), baseEvents...)
	mergedEvents = append(mergedEvents, mergeEvs...)
	report, err := VerifyReplay(dag, mergedEvents)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	if report.FinalStatus != RunStatusCompleted || report.CompletedTasks != report.TotalTasks {
		t.Fatalf("unexpected replay report: %+v", report)
	}
	if !reflect.DeepEqual(report.State.Tasks["publish"].Output, map[string]any{"v": "branch-pub"}) {
		t.Fatalf("publish output = %#v, want branch-pub", report.State.Tasks["publish"].Output)
	}
}

func TestMergeApplyClearsInheritedLeasesAndValidatesCmd(t *testing.T) {
	dag := mergeTestParseDAG(t)
	t0 := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)

	baseEvents, baseState := mergeTestBuildSharedPrefix(t, dag, t0)
	branchEvents, branchState := mergeTestForkFromPrefix(t, dag, baseEvents, "run-branch", t0.Add(2*time.Second))

	// Base leases branch_left (in-flight).
	mergeTestMustStep(t, dag, &baseEvents, baseState, CmdLeaseNextTask{WorkerID: "w-base"}, t0.Add(3*time.Second))
	if baseState.Tasks["branch_left"].Status != TaskStatusLeased {
		t.Fatalf("expected branch_left LEASED on base, got %s", baseState.Tasks["branch_left"].Status)
	}

	// Branch completes branch_right.
	mergeTestMustStep(t, dag, &branchEvents, branchState, CmdLeaseNextTask{WorkerID: "w-br-1"}, t0.Add(3*time.Second))
	mergeTestCompleteTask(t, dag, &branchEvents, branchState, "w-br-2", "branch_right", map[string]any{"right": "ok"}, t0.Add(4*time.Second))

	plan, err := PlanBranchMerge(dag, baseEvents, branchEvents, []string{"branch_right"}, MergeFailOnConflict)
	if err != nil {
		t.Fatalf("PlanBranchMerge: %v", err)
	}

	// Validation errors.
	if _, _, err := Step(dag, baseState, CmdMergeBranch{NewRunID: "", Plan: plan}, t0); err == nil {
		t.Fatal("expected error for empty NewRunID")
	}
	if _, _, err := Step(dag, baseState, CmdMergeBranch{NewRunID: "run-m", Plan: nil}, t0); err == nil {
		t.Fatal("expected error for nil Plan")
	}
	badPlan := &MergePlan{
		BaseRunID:    "run-base",
		BranchRunID:  "run-branch",
		AdoptedTasks: []AdoptedTask{{TaskID: "unknown_task"}},
	}
	if _, _, err := Step(dag, baseState, CmdMergeBranch{NewRunID: "run-m", Plan: badPlan}, t0); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound for unknown adopted task, got %v", err)
	}

	// Valid merge clears inherited active lease on branch_left.
	mergeEvs, _, err := Step(dag, baseState, CmdMergeBranch{
		NewRunID: "run-merged-leases",
		Plan:     plan,
	}, t0.Add(5*time.Second))
	if err != nil {
		t.Fatalf("Step(CmdMergeBranch): %v", err)
	}
	mergedEvents := append([]Event(nil), baseEvents...)
	mergedEvents = append(mergedEvents, mergeEvs...)

	report, err := VerifyReplay(dag, mergedEvents)
	if err != nil {
		t.Fatalf("VerifyReplay: %v", err)
	}
	bl := report.State.Tasks["branch_left"]
	if bl.Status != TaskStatusPending || bl.WorkerID != "" || bl.LeaseExpiresAtNs != 0 || bl.StartedAtNs != 0 || bl.RetryAfterNs != 0 {
		t.Fatalf("expected inherited lease on branch_left cleared to PENDING, got %+v", bl)
	}
	if report.State.Tasks["branch_right"].Status != TaskStatusCompleted {
		t.Fatalf("expected adopted branch_right COMPLETED, got %s", report.State.Tasks["branch_right"].Status)
	}
}
