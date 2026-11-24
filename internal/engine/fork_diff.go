package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type TaskDiff struct {
	TaskID        string     `json:"task_id"`
	StatusA       TaskStatus `json:"status_a"`
	StatusB       TaskStatus `json:"status_b"`
	AttemptsA     int        `json:"attempts_a"`
	AttemptsB     int        `json:"attempts_b"`
	AmbiguousA    int        `json:"ambiguous_a"`
	AmbiguousB    int        `json:"ambiguous_b"`
	OutputChanged bool       `json:"output_changed"`
}

type TimelineDiff struct {
	RunA            string     `json:"run_a"`
	RunB            string     `json:"run_b"`
	CommonPrefixSeq int64      `json:"common_prefix_seq"`
	DivergingA      []Event    `json:"diverging_a"`
	DivergingB      []Event    `json:"diverging_b"`
	TaskDeltas      []TaskDiff `json:"task_deltas"`
}

type ReplayReport struct {
	RunID           string    `json:"run_id"`
	EventsReplayed  int       `json:"events_replayed"`
	Checkpoints     int       `json:"checkpoints"`
	FinalStatus     RunStatus `json:"final_status"`
	CompletedTasks  int       `json:"completed_tasks"`
	TotalTasks      int       `json:"total_tasks"`
	AmbiguousEvents int       `json:"ambiguous_events"`
	State           *RunState `json:"state"`
}

// VerifyReplay reconstructs a run purely from its recorded event journal without
// executing any task handlers or external side effects, verifying monotonic
// sequence integrity, dependency ordering on leases, strictly increasing fencing
// tokens, and state consistency.
func VerifyReplay(dag *DAG, events []Event) (*ReplayReport, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("cannot fold empty event stream")
	}
	runID := events[len(events)-1].RunID
	state := NewRunState(dag, runID)
	var lastLeaseFencingToken int64

	for i, ev := range events {
		expectedSeq := int64(i + 1)
		if ev.Seq != expectedSeq {
			return nil, fmt.Errorf("non-monotonic event seq at index %d: got %d, expected %d", i, ev.Seq, expectedSeq)
		}
		if ev.Type == EventTaskLeased {
			if ev.FencingToken <= lastLeaseFencingToken {
				return nil, fmt.Errorf("non-increasing fencing_token on TASK_LEASED at seq=%d task=%s: got %d, previous %d", ev.Seq, ev.TaskID, ev.FencingToken, lastLeaseFencingToken)
			}
			lastLeaseFencingToken = ev.FencingToken

			spec, ok := dag.TasksByID[ev.TaskID]
			if !ok {
				return nil, fmt.Errorf("apply event seq=%d type=%s: %w: %s", ev.Seq, ev.Type, ErrTaskNotFound, ev.TaskID)
			}
			for _, dep := range spec.DependsOn {
				depState := state.Tasks[dep]
				if depState == nil || depState.Status != TaskStatusCompleted {
					status := TaskStatus("")
					if depState != nil {
						status = depState.Status
					}
					return nil, fmt.Errorf("dependency violation at seq=%d: task %q leased before dependency %q completed (status=%s)", ev.Seq, ev.TaskID, dep, status)
				}
			}
		}
		if err := Apply(state, ev); err != nil {
			return nil, fmt.Errorf("apply event seq=%d type=%s: %w", ev.Seq, ev.Type, err)
		}
	}

	completed := 0
	ambiguous := 0
	for _, id := range state.TopoOrder {
		ts := state.Tasks[id]
		if ts.Status == TaskStatusCompleted {
			completed++
		}
		ambiguous += ts.AmbiguousCount
	}
	return &ReplayReport{
		RunID:           state.RunID,
		EventsReplayed:  len(events),
		Checkpoints:     len(state.Checkpoints),
		FinalStatus:     state.Status,
		CompletedTasks:  completed,
		TotalTasks:      len(state.TopoOrder),
		AmbiguousEvents: ambiguous,
		State:           state,
	}, nil
}

// DiffRuns compares two runs (including single- or multi-hop forks) by finding
// their longest identical prefix in the resolved event streams and computing
// diverging events and per-task state/output deltas.
func DiffRuns(dag *DAG, eventsA, eventsB []Event) (*TimelineDiff, error) {
	stateA, err := Fold(dag, eventsA)
	if err != nil {
		return nil, fmt.Errorf("fold run A: %w", err)
	}
	stateB, err := Fold(dag, eventsB)
	if err != nil {
		return nil, fmt.Errorf("fold run B: %w", err)
	}

	minLen := len(eventsA)
	if len(eventsB) < minLen {
		minLen = len(eventsB)
	}
	common := 0
	for common < minLen && eventsEqual(eventsA[common], eventsB[common]) {
		common++
	}

	var commonSeq int64
	if common > 0 {
		commonSeq = eventsA[common-1].Seq
	}

	var deltas []TaskDiff
	for _, id := range dag.TopoOrder {
		ta, tb := stateA.Tasks[id], stateB.Tasks[id]
		outChanged := !jsonEqual(ta.Output, tb.Output)
		if ta.Status != tb.Status || ta.Attempt != tb.Attempt || ta.AmbiguousCount != tb.AmbiguousCount || outChanged {
			deltas = append(deltas, TaskDiff{
				TaskID:        id,
				StatusA:       ta.Status,
				StatusB:       tb.Status,
				AttemptsA:     ta.Attempt,
				AttemptsB:     tb.Attempt,
				AmbiguousA:    ta.AmbiguousCount,
				AmbiguousB:    tb.AmbiguousCount,
				OutputChanged: outChanged,
			})
		}
	}

	return &TimelineDiff{
		RunA:            stateA.RunID,
		RunB:            stateB.RunID,
		CommonPrefixSeq: commonSeq,
		DivergingA:      append([]Event(nil), eventsA[common:]...),
		DivergingB:      append([]Event(nil), eventsB[common:]...),
		TaskDeltas:      deltas,
	}, nil
}

// FormatDiff renders a compact, human-readable comparison of two timelines.
func FormatDiff(d *TimelineDiff) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Timeline Diff: %s vs %s\n", d.RunA, d.RunB)
	fmt.Fprintf(&b, "Common prefix through seq: %d\n", d.CommonPrefixSeq)
	fmt.Fprintf(&b, "Diverging events: %s=%d, %s=%d\n", d.RunA, len(d.DivergingA), d.RunB, len(d.DivergingB))
	if len(d.TaskDeltas) == 0 {
		b.WriteString("Task deltas: none (identical final task states and outputs)\n")
		return b.String()
	}
	b.WriteString("Task deltas:\n")
	for _, td := range d.TaskDeltas {
		fmt.Fprintf(&b, "  - %s: %s(att=%d,amb=%d) -> %s(att=%d,amb=%d) output_changed=%v\n",
			td.TaskID, td.StatusA, td.AttemptsA, td.AmbiguousA, td.StatusB, td.AttemptsB, td.AmbiguousB, td.OutputChanged)
	}
	return b.String()
}

func eventsEqual(a, b Event) bool {
	return a.Seq == b.Seq &&
		a.Type == b.Type &&
		a.TaskID == b.TaskID &&
		a.Attempt == b.Attempt &&
		a.FencingToken == b.FencingToken &&
		a.WorkerID == b.WorkerID &&
		bytes.Equal(a.Payload, b.Payload)
}

func jsonEqual(a, b map[string]any) bool {
	ba, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ba, bb)
}
