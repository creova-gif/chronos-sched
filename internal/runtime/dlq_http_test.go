package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

const testDLQWorkflowYAML = `name: dlq-http-test-wf
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

func newDLQHTTPTestMux(t *testing.T) (*Coordinator, *sqlite.Store, *http.ServeMux) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "dlq-http.db")
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
	mux.HandleFunc("GET /v1/dlq", coord.handleListDeadLetters)
	mux.HandleFunc("POST /v1/dlq/{id}/requeue", coord.handleRequeueDeadLetter)
	return coord, st, mux
}

func doDLQRequest(t *testing.T, handler http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestDLQHTTP_ListAndRequeueLifecycle(t *testing.T) {
	coord, st, mux := newDLQHTTPTestMux(t)
	ctx := context.Background()

	// Empty list returns [] rather than null.
	emptyRec := doDLQRequest(t, mux, http.MethodGet, "/v1/dlq")
	if emptyRec.Code != http.StatusOK {
		t.Fatalf("empty GET /v1/dlq status = %d, want 200: %s", emptyRec.Code, emptyRec.Body.String())
	}
	var emptyResp ListDeadLettersResponse
	if err := json.Unmarshal(emptyRec.Body.Bytes(), &emptyResp); err != nil {
		t.Fatalf("unmarshal empty response: %v", err)
	}
	if emptyResp.Entries == nil || len(emptyResp.Entries) != 0 {
		t.Fatalf("expected non-nil empty entries slice, got %#v", emptyResp.Entries)
	}

	// Submit a workflow so the original RunRecord exists for requeueing.
	subResp, err := coord.SubmitWorkflow(ctx, SubmitWorkflowRequest{
		RunID:        "run-dlq-1",
		WorkflowYAML: testDLQWorkflowYAML,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	// Record a dead letter for the run.
	if err := st.RecordDeadLetter(ctx, store.DeadLetterEntry{
		RunID:        subResp.RunID,
		TaskID:       "step1",
		WorkflowName: subResp.WorkflowName,
		Attempt:      3,
		Reason:       "max retries exhausted",
		FailedAtNs:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	// List active dead letters via GET /v1/dlq.
	listRec := doDLQRequest(t, mux, http.MethodGet, "/v1/dlq")
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /v1/dlq status = %d, want 200: %s", listRec.Code, listRec.Body.String())
	}
	var listResp ListDeadLettersResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(listResp.Entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(listResp.Entries))
	}
	entry := listResp.Entries[0]
	if entry.RunID != subResp.RunID || entry.TaskID != "step1" || entry.Requeued {
		t.Fatalf("unexpected entry: %+v", entry)
	}

	// Requeue via POST /v1/dlq/{id}/requeue.
	requeuePath := fmt.Sprintf("/v1/dlq/%d/requeue", entry.ID)
	reqRec := doDLQRequest(t, mux, http.MethodPost, requeuePath)
	if reqRec.Code != http.StatusCreated {
		t.Fatalf("POST %s status = %d, want 201: %s", requeuePath, reqRec.Code, reqRec.Body.String())
	}
	var reqResp RequeueDeadLetterResponse
	if err := json.Unmarshal(reqRec.Body.Bytes(), &reqResp); err != nil {
		t.Fatalf("unmarshal requeue response: %v", err)
	}
	if reqResp.DeadLetterID != entry.ID {
		t.Fatalf("dead_letter_id = %d, want %d", reqResp.DeadLetterID, entry.ID)
	}
	if reqResp.RequeuedRunID == "" || reqResp.RequeuedRunID == subResp.RunID {
		t.Fatalf("unexpected requeued_run_id %q", reqResp.RequeuedRunID)
	}

	// Verify the newly submitted run exists in store.
	if _, err := st.GetRun(ctx, reqResp.RequeuedRunID); err != nil {
		t.Fatalf("GetRun(%s): %v", reqResp.RequeuedRunID, err)
	}

	// GET /v1/dlq excludes requeued entries.
	activeAfter := doDLQRequest(t, mux, http.MethodGet, "/v1/dlq")
	if activeAfter.Code != http.StatusOK {
		t.Fatalf("GET /v1/dlq after requeue status = %d, want 200", activeAfter.Code)
	}
	var activeAfterResp ListDeadLettersResponse
	if err := json.Unmarshal(activeAfter.Body.Bytes(), &activeAfterResp); err != nil {
		t.Fatalf("unmarshal activeAfter: %v", err)
	}
	if len(activeAfterResp.Entries) != 0 {
		t.Fatalf("expected 0 active entries after requeue, got %d", len(activeAfterResp.Entries))
	}

	// GET /v1/dlq?all=true includes the requeued entry with requeued: true.
	allAfter := doDLQRequest(t, mux, http.MethodGet, "/v1/dlq?all=true")
	if allAfter.Code != http.StatusOK {
		t.Fatalf("GET /v1/dlq?all=true status = %d, want 200", allAfter.Code)
	}
	var allAfterResp ListDeadLettersResponse
	if err := json.Unmarshal(allAfter.Body.Bytes(), &allAfterResp); err != nil {
		t.Fatalf("unmarshal allAfter: %v", err)
	}
	if len(allAfterResp.Entries) != 1 {
		t.Fatalf("expected 1 entry with ?all=true, got %d", len(allAfterResp.Entries))
	}
	if !allAfterResp.Entries[0].Requeued || allAfterResp.Entries[0].RequeuedRunID != reqResp.RequeuedRunID {
		t.Fatalf("expected requeued=true and requeued_run_id=%q, got %+v", reqResp.RequeuedRunID, allAfterResp.Entries[0])
	}

	// Duplicate requeue returns 409 Conflict.
	dupRec := doDLQRequest(t, mux, http.MethodPost, requeuePath)
	if dupRec.Code != http.StatusConflict {
		t.Fatalf("duplicate POST %s status = %d, want 409: %s", requeuePath, dupRec.Code, dupRec.Body.String())
	}

	// Missing ID returns 404 Not Found.
	missingRec := doDLQRequest(t, mux, http.MethodPost, "/v1/dlq/99999/requeue")
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing ID status = %d, want 404: %s", missingRec.Code, missingRec.Body.String())
	}

	// Invalid ID returns 400 Bad Request.
	badRec := doDLQRequest(t, mux, http.MethodPost, "/v1/dlq/not-an-int/requeue")
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid ID status = %d, want 400: %s", badRec.Code, badRec.Body.String())
	}
}
