package engine_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

func newApprovalTestState(taskID string, status engine.TaskStatus) *engine.RunState {
	return &engine.RunState{
		RunID:        "run-approval-1",
		WorkflowName: "approval-wf",
		WorkflowHash: "hash-1",
		Status:       engine.RunStatusRunning,
		LastSeq:      1,
		Tasks: map[string]*engine.TaskState{
			taskID: {
				ID:     taskID,
				Status: status,
			},
		},
		TopoOrder: []string{taskID},
	}
}

func TestApprovalGateApproveAndReplay(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	live := newApprovalTestState("deploy-prod", engine.TaskStatusPending)
	replayed := newApprovalTestState("deploy-prod", engine.TaskStatusPending)

	reqEv, err := engine.RequestTaskApproval(live, "deploy-prod", "alice", "needs sign-off", now)
	if err != nil {
		t.Fatalf("RequestTaskApproval failed: %v", err)
	}
	if reqEv.Type != engine.EventTaskApprovalRequested {
		t.Fatalf("expected event %s, got %s", engine.EventTaskApprovalRequested, reqEv.Type)
	}
	if reqEv.Seq != 2 || live.LastSeq != 2 {
		t.Fatalf("expected seq 2, got event seq=%d state.LastSeq=%d", reqEv.Seq, live.LastSeq)
	}
	if live.Tasks["deploy-prod"].Status != engine.TaskStatusWaitingApproval {
		t.Fatalf("expected WAITING_APPROVAL, got %s", live.Tasks["deploy-prod"].Status)
	}

	appEv, err := engine.ResolveTaskApproval(live, "deploy-prod", "bob", "LGTM", true, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ResolveTaskApproval(approve) failed: %v", err)
	}
	if appEv.Type != engine.EventTaskApproved {
		t.Fatalf("expected event %s, got %s", engine.EventTaskApproved, appEv.Type)
	}
	if appEv.Seq != 3 || live.LastSeq != 3 {
		t.Fatalf("expected seq 3, got event seq=%d state.LastSeq=%d", appEv.Seq, live.LastSeq)
	}
	if live.Tasks["deploy-prod"].Status != engine.TaskStatusPending {
		t.Fatalf("expected PENDING after approval, got %s", live.Tasks["deploy-prod"].Status)
	}

	var payload engine.ApprovalPayload
	if err := json.Unmarshal(appEv.Payload, &payload); err != nil {
		t.Fatalf("unmarshal approval payload: %v", err)
	}
	if payload.TaskID != "deploy-prod" || payload.Actor != "bob" || payload.Comment != "LGTM" || !payload.Approved {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	for _, ev := range []engine.Event{reqEv, appEv} {
		if err := engine.ApplyApprovalEvent(replayed, ev); err != nil {
			t.Fatalf("ApplyApprovalEvent(%s) failed: %v", ev.Type, err)
		}
	}
	if !reflect.DeepEqual(live, replayed) {
		t.Fatalf("replayed state mismatch:\nlive:     %+v\nreplayed: %+v", live, replayed)
	}
}

func TestApprovalGateRejectAndInvalidTransitions(t *testing.T) {
	now := time.Unix(1700000100, 0).UTC()
	live := newApprovalTestState("deploy-prod", engine.TaskStatusPending)
	replayed := newApprovalTestState("deploy-prod", engine.TaskStatusPending)

	// Cannot resolve approval before it is requested.
	if _, err := engine.ResolveTaskApproval(live, "deploy-prod", "bob", "too early", true, now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition resolving PENDING task, got %v", err)
	}

	// Nil state and unknown task return expected errors.
	if _, err := engine.RequestTaskApproval(nil, "deploy-prod", "alice", "", now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition for nil state, got %v", err)
	}
	if _, err := engine.RequestTaskApproval(live, "missing-task", "alice", "", now); !errors.Is(err, engine.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound for missing task, got %v", err)
	}

	reqEv, err := engine.RequestTaskApproval(live, "deploy-prod", "alice", "please review", now)
	if err != nil {
		t.Fatalf("RequestTaskApproval failed: %v", err)
	}
	// Cannot request approval twice.
	if _, err := engine.RequestTaskApproval(live, "deploy-prod", "alice", "again", now); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition on double request, got %v", err)
	}

	rejEv, err := engine.ResolveTaskApproval(live, "deploy-prod", "bob", "missing rollback plan", false, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ResolveTaskApproval(reject) failed: %v", err)
	}
	if rejEv.Type != engine.EventTaskRejected {
		t.Fatalf("expected event %s, got %s", engine.EventTaskRejected, rejEv.Type)
	}
	if live.Tasks["deploy-prod"].Status != engine.TaskStatusFailed {
		t.Fatalf("expected FAILED after rejection, got %s", live.Tasks["deploy-prod"].Status)
	}
	if live.Tasks["deploy-prod"].LastError != "missing rollback plan" {
		t.Fatalf("expected LastError %q, got %q", "missing rollback plan", live.Tasks["deploy-prod"].LastError)
	}

	// Cannot resolve an already rejected/failed task.
	if _, err := engine.ResolveTaskApproval(live, "deploy-prod", "bob", "retry", true, now.Add(2*time.Minute)); !errors.Is(err, engine.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition resolving FAILED task, got %v", err)
	}

	for _, ev := range []engine.Event{reqEv, rejEv} {
		if err := engine.ApplyApprovalEvent(replayed, ev); err != nil {
			t.Fatalf("ApplyApprovalEvent(%s) failed: %v", ev.Type, err)
		}
	}
	if !reflect.DeepEqual(live, replayed) {
		t.Fatalf("replayed state mismatch:\nlive:     %+v\nreplayed: %+v", live, replayed)
	}
}

func TestApprovalGateFoldAndVerifyReplay(t *testing.T) {
	t.Parallel()

	const workflowYAML = `
name: approval-replay-wf
tasks:
  - id: build
    handler: sim
  - id: deploy-prod
    handler: sim
    depends_on: [build]
`
	dag, err := engine.ParseDAG([]byte(workflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG failed: %v", err)
	}

	now := time.Unix(1700000200, 0).UTC()

	cases := []struct {
		name           string
		runID          string
		approved       bool
		comment        string
		wantFinal      engine.TaskStatus
		wantLastError  string
		wantFinalEvent engine.EventType
	}{
		{
			name:           "approved",
			runID:          "run-approval-fold-approved",
			approved:       true,
			comment:        "LGTM",
			wantFinal:      engine.TaskStatusPending,
			wantLastError:  "",
			wantFinalEvent: engine.EventTaskApproved,
		},
		{
			name:           "rejected",
			runID:          "run-approval-fold-rejected",
			approved:       false,
			comment:        "missing rollback plan",
			wantFinal:      engine.TaskStatusFailed,
			wantLastError:  "missing rollback plan",
			wantFinalEvent: engine.EventTaskRejected,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			live := engine.NewRunState(dag, tc.runID)
			startEvs, _, err := engine.Step(dag, live, engine.CmdStartRun{}, now)
			if err != nil {
				t.Fatalf("Step(CmdStartRun) failed: %v", err)
			}
			for _, ev := range startEvs {
				if err := engine.Apply(live, ev); err != nil {
					t.Fatalf("Apply(%s) failed: %v", ev.Type, err)
				}
			}

			reqEv, err := engine.RequestTaskApproval(live, "deploy-prod", "alice", "needs sign-off", now.Add(time.Second))
			if err != nil {
				t.Fatalf("RequestTaskApproval failed: %v", err)
			}
			resEv, err := engine.ResolveTaskApproval(live, "deploy-prod", "bob", tc.comment, tc.approved, now.Add(2*time.Second))
			if err != nil {
				t.Fatalf("ResolveTaskApproval failed: %v", err)
			}
			if resEv.Type != tc.wantFinalEvent {
				t.Fatalf("expected event %s, got %s", tc.wantFinalEvent, resEv.Type)
			}

			journal := append(append([]engine.Event(nil), startEvs...), reqEv, resEv)

			midState, err := engine.StateAtSeq(dag, journal, reqEv.Seq)
			if err != nil {
				t.Fatalf("StateAtSeq(%d) failed: %v", reqEv.Seq, err)
			}
			if got := midState.Tasks["deploy-prod"].Status; got != engine.TaskStatusWaitingApproval {
				t.Fatalf("expected %s at seq %d, got %s", engine.TaskStatusWaitingApproval, reqEv.Seq, got)
			}

			folded1, err := engine.Fold(dag, journal)
			if err != nil {
				t.Fatalf("Fold(1) failed: %v", err)
			}
			folded2, err := engine.Fold(dag, journal)
			if err != nil {
				t.Fatalf("Fold(2) failed: %v", err)
			}
			if !reflect.DeepEqual(folded1, folded2) {
				t.Fatalf("Fold is not deterministic:\nfirst:  %+v\nsecond: %+v", folded1, folded2)
			}
			if !reflect.DeepEqual(live, folded1) {
				t.Fatalf("folded state mismatch:\nlive:   %+v\nfolded: %+v", live, folded1)
			}
			if got := folded1.Tasks["deploy-prod"].Status; got != tc.wantFinal {
				t.Fatalf("expected %s at final seq, got %s", tc.wantFinal, got)
			}
			if got := folded1.Tasks["deploy-prod"].LastError; got != tc.wantLastError {
				t.Fatalf("expected LastError %q, got %q", tc.wantLastError, got)
			}

			finalAtSeq, err := engine.StateAtSeq(dag, journal, resEv.Seq)
			if err != nil {
				t.Fatalf("StateAtSeq(%d) failed: %v", resEv.Seq, err)
			}
			if !reflect.DeepEqual(folded1, finalAtSeq) {
				t.Fatalf("StateAtSeq(%d) mismatch vs Fold:\nfold: %+v\nseq:  %+v", resEv.Seq, folded1, finalAtSeq)
			}

			report, err := engine.VerifyReplay(dag, journal)
			if err != nil {
				t.Fatalf("VerifyReplay failed: %v", err)
			}
			if report.EventsReplayed != len(journal) {
				t.Fatalf("expected EventsReplayed %d, got %d", len(journal), report.EventsReplayed)
			}
			if !reflect.DeepEqual(folded1, report.State) {
				t.Fatalf("VerifyReplay State mismatch:\nfolded: %+v\nreport: %+v", folded1, report.State)
			}
		})
	}
}
