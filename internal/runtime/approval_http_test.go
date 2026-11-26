package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

const testApprovalWorkflowYAML = `name: approval-http-test-wf
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

func TestApprovalHTTP_RequestApproveAndReject(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "approval-http.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/request-approval", coord.handleRequestTaskApproval)
	mux.HandleFunc("POST /v1/runs/{id}/tasks/{task_id}/approve", coord.handleResolveTaskApproval)

	doApprovalReq := func(target string, body any) *httptest.ResponseRecorder {
		t.Helper()
		var buf bytes.Buffer
		if body != nil {
			if err := json.NewEncoder(&buf).Encode(body); err != nil {
				t.Fatalf("encode request body: %v", err)
			}
		}
		req := httptest.NewRequest(http.MethodPost, target, &buf)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	// Missing run returns 404.
	notFoundRec := doApprovalReq("/v1/runs/missing-run/tasks/step1/request-approval", nil)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("missing run status = %d, want 404 (%s)", notFoundRec.Code, notFoundRec.Body.String())
	}

	// Run 1: request approval (with empty body) -> WAITING_APPROVAL, then approve -> PENDING.
	sub1, err := coord.SubmitWorkflow(ctx, SubmitWorkflowRequest{
		RunID:        "run-approve-1",
		WorkflowYAML: testApprovalWorkflowYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow(run-approve-1): %v", err)
	}

	// Missing task returns 409.
	missingTaskRec := doApprovalReq("/v1/runs/"+sub1.RunID+"/tasks/no-such-task/request-approval", nil)
	if missingTaskRec.Code != http.StatusConflict {
		t.Fatalf("missing task status = %d, want 409 (%s)", missingTaskRec.Code, missingTaskRec.Body.String())
	}

	reqRec1 := doApprovalReq("/v1/runs/"+sub1.RunID+"/tasks/step1/request-approval", nil)
	if reqRec1.Code != http.StatusOK {
		t.Fatalf("request-approval status = %d, want 200 (%s)", reqRec1.Code, reqRec1.Body.String())
	}
	var reqResp1 ApprovalResponse
	if err := json.NewDecoder(reqRec1.Body).Decode(&reqResp1); err != nil {
		t.Fatalf("decode request-approval response: %v", err)
	}
	if reqResp1.RunID != sub1.RunID || reqResp1.TaskID != "step1" || reqResp1.TaskStatus != engine.TaskStatusWaitingApproval {
		t.Fatalf("unexpected request-approval response: %+v", reqResp1)
	}

	approveRec := doApprovalReq("/v1/runs/"+sub1.RunID+"/tasks/step1/approve", ApprovalRequest{
		Actor:    "alice",
		Comment:  "looks good",
		Approved: true,
	})
	if approveRec.Code != http.StatusOK {
		t.Fatalf("approve status = %d, want 200 (%s)", approveRec.Code, approveRec.Body.String())
	}
	var approveResp ApprovalResponse
	if err := json.NewDecoder(approveRec.Body).Decode(&approveResp); err != nil {
		t.Fatalf("decode approve response: %v", err)
	}
	if approveResp.TaskStatus != engine.TaskStatusPending || approveResp.EventSeq != reqResp1.EventSeq+1 {
		t.Fatalf("unexpected approve response: %+v", approveResp)
	}

	events1, err := st.LoadRunEvents(ctx, sub1.RunID)
	if err != nil {
		t.Fatalf("LoadRunEvents(run-approve-1): %v", err)
	}
	if len(events1) < 2 {
		t.Fatalf("expected at least 2 events, got %d", len(events1))
	}
	lastTwo1 := events1[len(events1)-2:]
	if lastTwo1[0].Type != engine.EventTaskApprovalRequested || lastTwo1[1].Type != engine.EventTaskApproved {
		t.Fatalf("unexpected persisted event types for run-approve-1: %s, %s", lastTwo1[0].Type, lastTwo1[1].Type)
	}

	// Run 2: request approval -> WAITING_APPROVAL, then reject -> FAILED with LastError.
	sub2, err := coord.SubmitWorkflow(ctx, SubmitWorkflowRequest{
		RunID:        "run-reject-1",
		WorkflowYAML: testApprovalWorkflowYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow(run-reject-1): %v", err)
	}

	reqRec2 := doApprovalReq("/v1/runs/"+sub2.RunID+"/tasks/step1/request-approval", ApprovalRequest{
		Actor:   "system",
		Comment: "gate check",
	})
	if reqRec2.Code != http.StatusOK {
		t.Fatalf("run-reject-1 request-approval status = %d, want 200 (%s)", reqRec2.Code, reqRec2.Body.String())
	}

	rejectRec := doApprovalReq("/v1/runs/"+sub2.RunID+"/tasks/step1/approve", ApprovalRequest{
		Actor:    "bob",
		Comment:  "invalid config",
		Approved: false,
	})
	if rejectRec.Code != http.StatusOK {
		t.Fatalf("reject status = %d, want 200 (%s)", rejectRec.Code, rejectRec.Body.String())
	}
	var rejectResp ApprovalResponse
	if err := json.NewDecoder(rejectRec.Body).Decode(&rejectResp); err != nil {
		t.Fatalf("decode reject response: %v", err)
	}
	if rejectResp.TaskStatus != engine.TaskStatusFailed {
		t.Fatalf("reject TaskStatus = %s, want %s", rejectResp.TaskStatus, engine.TaskStatusFailed)
	}

	coord.mu.Lock()
	lastErr := coord.runs[sub2.RunID].state.Tasks["step1"].LastError
	coord.mu.Unlock()
	if lastErr != "invalid config" {
		t.Fatalf("rejected task LastError = %q, want %q", lastErr, "invalid config")
	}

	events2, err := st.LoadRunEvents(ctx, sub2.RunID)
	if err != nil {
		t.Fatalf("LoadRunEvents(run-reject-1): %v", err)
	}
	if len(events2) < 2 {
		t.Fatalf("expected at least 2 events for run-reject-1, got %d", len(events2))
	}
	lastTwo2 := events2[len(events2)-2:]
	if lastTwo2[0].Type != engine.EventTaskApprovalRequested || lastTwo2[1].Type != engine.EventTaskRejected {
		t.Fatalf("unexpected persisted event types for run-reject-1: %s, %s", lastTwo2[0].Type, lastTwo2[1].Type)
	}
}
