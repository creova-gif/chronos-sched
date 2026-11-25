package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var ErrMergeConflict = errors.New("timeline merge conflict")

type MergeStrategy string

const (
	MergeFailOnConflict MergeStrategy = "fail_on_conflict"
	MergePreferBranch   MergeStrategy = "prefer_branch"
)

type MergeConflict struct {
	TaskID             string         `json:"task_id"`
	BaseStatus         TaskStatus     `json:"base_status"`
	BranchStatus       TaskStatus     `json:"branch_status"`
	BaseCompletedSeq   int64          `json:"base_completed_seq,omitempty"`
	BranchCompletedSeq int64          `json:"branch_completed_seq,omitempty"`
	BaseOutput         map[string]any `json:"base_output,omitempty"`
	BranchOutput       map[string]any `json:"branch_output,omitempty"`
	Reason             string         `json:"reason"`
}

type AdoptedTask struct {
	TaskID             string         `json:"task_id"`
	Output             map[string]any `json:"output,omitempty"`
	BranchCompletedSeq int64          `json:"branch_completed_seq,omitempty"`
}

type MergePlan struct {
	BaseRunID             string          `json:"base_run_id"`
	BranchRunID           string          `json:"branch_run_id"`
	CommonPrefixSeq       int64           `json:"common_prefix_seq"`
	Strategy              MergeStrategy   `json:"strategy"`
	AdoptedTasks          []AdoptedTask   `json:"adopted_tasks,omitempty"`
	Conflicts             []MergeConflict `json:"conflicts,omitempty"`
	InvalidatedDownstream []string        `json:"invalidated_downstream,omitempty"`
}

// PlanBranchMerge computes a deterministic Git-style merge or cherry-pick plan
// from branchEvents onto baseEvents. If taskIDs is empty, all tasks completed on
// branch whose completion or output diverged after CommonPrefixSeq are selected.
// If taskIDs is non-empty, every requested task must exist in dag and be
// completed on branch. When strategy is MergeFailOnConflict and conflicts exist,
// the populated MergePlan is returned alongside ErrMergeConflict.
func PlanBranchMerge(dag *DAG, baseEvents, branchEvents []Event, taskIDs []string, strategy MergeStrategy) (*MergePlan, error) {
	if dag == nil {
		return nil, fmt.Errorf("dag is required")
	}
	if strategy == "" {
		strategy = MergeFailOnConflict
	}
	if strategy != MergeFailOnConflict && strategy != MergePreferBranch {
		return nil, fmt.Errorf("unsupported merge strategy %q", strategy)
	}

	diff, err := DiffRuns(dag, baseEvents, branchEvents)
	if err != nil {
		return nil, fmt.Errorf("diff runs: %w", err)
	}
	baseState, err := Fold(dag, baseEvents)
	if err != nil {
		return nil, fmt.Errorf("fold base run: %w", err)
	}
	branchState, err := Fold(dag, branchEvents)
	if err != nil {
		return nil, fmt.Errorf("fold branch run: %w", err)
	}

	selected, err := selectMergeTasks(dag, baseState, branchState, diff.CommonPrefixSeq, taskIDs)
	if err != nil {
		return nil, err
	}

	plan := &MergePlan{
		BaseRunID:       baseState.RunID,
		BranchRunID:     branchState.RunID,
		CommonPrefixSeq: diff.CommonPrefixSeq,
		Strategy:        strategy,
	}

	for _, id := range selected {
		baseTS := baseState.Tasks[id]
		branchTS := branchState.Tasks[id]

		hasConflict := baseTS.Status == TaskStatusCompleted &&
			baseTS.CompletedSeq > diff.CommonPrefixSeq &&
			!jsonEqual(baseTS.Output, branchTS.Output)

		if hasConflict {
			plan.Conflicts = append(plan.Conflicts, MergeConflict{
				TaskID:             id,
				BaseStatus:         baseTS.Status,
				BranchStatus:       branchTS.Status,
				BaseCompletedSeq:   baseTS.CompletedSeq,
				BranchCompletedSeq: branchTS.CompletedSeq,
				BaseOutput:         cloneMap(baseTS.Output),
				BranchOutput:       cloneMap(branchTS.Output),
				Reason:             fmt.Sprintf("task %q completed on both base (seq=%d) and branch (seq=%d) with differing outputs", id, baseTS.CompletedSeq, branchTS.CompletedSeq),
			})
			if strategy == MergeFailOnConflict {
				continue
			}
		}

		plan.AdoptedTasks = append(plan.AdoptedTasks, AdoptedTask{
			TaskID:             id,
			Output:             cloneMap(branchTS.Output),
			BranchCompletedSeq: branchTS.CompletedSeq,
		})
	}

	plan.InvalidatedDownstream = computeInvalidatedDownstream(dag, plan.AdoptedTasks)

	if strategy == MergeFailOnConflict && len(plan.Conflicts) > 0 {
		return plan, fmt.Errorf("%w: %d conflicting task(s) between base %q and branch %q",
			ErrMergeConflict, len(plan.Conflicts), plan.BaseRunID, plan.BranchRunID)
	}
	return plan, nil
}

func selectMergeTasks(dag *DAG, baseState, branchState *RunState, commonPrefixSeq int64, taskIDs []string) ([]string, error) {
	if len(taskIDs) > 0 {
		requested := make(map[string]struct{}, len(taskIDs))
		for _, id := range taskIDs {
			if _, ok := dag.TasksByID[id]; !ok {
				return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, id)
			}
			branchTS := branchState.Tasks[id]
			if branchTS == nil || branchTS.Status != TaskStatusCompleted {
				status := TaskStatus("")
				if branchTS != nil {
					status = branchTS.Status
				}
				return nil, fmt.Errorf("cannot cherry-pick task %q from branch %q: status is %s, expected %s",
					id, branchState.RunID, status, TaskStatusCompleted)
			}
			requested[id] = struct{}{}
		}
		selected := make([]string, 0, len(requested))
		for _, id := range dag.TopoOrder {
			if _, ok := requested[id]; ok {
				selected = append(selected, id)
			}
		}
		return selected, nil
	}

	var selected []string
	for _, id := range dag.TopoOrder {
		baseTS := baseState.Tasks[id]
		branchTS := branchState.Tasks[id]
		if branchTS.Status != TaskStatusCompleted {
			continue
		}
		if branchTS.CompletedSeq <= commonPrefixSeq {
			continue
		}
		if baseTS.Status != TaskStatusCompleted || !jsonEqual(baseTS.Output, branchTS.Output) {
			selected = append(selected, id)
		}
	}
	return selected, nil
}

func computeInvalidatedDownstream(dag *DAG, adopted []AdoptedTask) []string {
	if len(adopted) == 0 {
		return nil
	}

	adoptedSet := make(map[string]struct{}, len(adopted))
	queue := make([]string, 0, len(adopted))
	for _, a := range adopted {
		adoptedSet[a.TaskID] = struct{}{}
		queue = append(queue, a.TaskID)
	}

	visited := make(map[string]struct{})
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for _, dep := range dag.Dependents[curr] {
			if _, seen := visited[dep]; seen {
				continue
			}
			visited[dep] = struct{}{}
			queue = append(queue, dep)
		}
	}

	var invalidated []string
	for _, id := range dag.TopoOrder {
		if _, isDownstream := visited[id]; !isDownstream {
			continue
		}
		if _, isAdopted := adoptedSet[id]; isAdopted {
			continue
		}
		invalidated = append(invalidated, id)
	}
	return invalidated
}

// FormatMergePlan renders a crisp Git-style merge preview for human inspection.
func FormatMergePlan(p *MergePlan) string {
	if p == nil {
		return "Timeline Merge Plan: <nil>\n"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Timeline Merge Plan: %s <- %s\n", p.BaseRunID, p.BranchRunID)
	fmt.Fprintf(&b, "Common prefix through seq: %d\n", p.CommonPrefixSeq)
	fmt.Fprintf(&b, "Strategy: %s\n", p.Strategy)

	if len(p.AdoptedTasks) == 0 {
		b.WriteString("Adopted tasks: none\n")
	} else {
		fmt.Fprintf(&b, "Adopted tasks (%d):\n", len(p.AdoptedTasks))
		for _, t := range p.AdoptedTasks {
			fmt.Fprintf(&b, "  + %s (branch_seq=%d, output=%s)\n", t.TaskID, t.BranchCompletedSeq, formatJSONCompact(t.Output))
		}
	}

	if len(p.Conflicts) == 0 {
		b.WriteString("Conflicts: none\n")
	} else {
		fmt.Fprintf(&b, "Conflicts (%d):\n", len(p.Conflicts))
		for _, c := range p.Conflicts {
			fmt.Fprintf(&b, "  ! %s: base(seq=%d, output=%s) vs branch(seq=%d, output=%s)\n",
				c.TaskID, c.BaseCompletedSeq, formatJSONCompact(c.BaseOutput), c.BranchCompletedSeq, formatJSONCompact(c.BranchOutput))
		}
	}

	if len(p.InvalidatedDownstream) == 0 {
		b.WriteString("Invalidated downstream: none\n")
	} else {
		fmt.Fprintf(&b, "Invalidated downstream (%d):\n", len(p.InvalidatedDownstream))
		for _, id := range p.InvalidatedDownstream {
			fmt.Fprintf(&b, "  ~ %s (re-execute)\n", id)
		}
	}

	return b.String()
}

func formatJSONCompact(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}
