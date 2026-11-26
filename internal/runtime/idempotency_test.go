package runtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

const testIdempotencyWorkflowYAML1 = `name: idempotency-wf-1
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

const testIdempotencyWorkflowYAML2 = `name: idempotency-wf-2
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 20
`

func postSubmitWorkflowWithIdempotency(t *testing.T, handler http.Handler, tenant, idemKey, wfYAML string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(SubmitWorkflowRequest{WorkflowYAML: wfYAML})
	if err != nil {
		t.Fatalf("marshal submit request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" {
		req.Header.Set(HeaderTenant, tenant)
	}
	if idemKey != "" {
		req.Header.Set(headerIdempotencyKey, idemKey)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestCoordinator_IdempotencyKeySubmit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotency.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	handler := coord.Handler()

	// 1. First submission with Idempotency-Key returns 201 Created.
	rec1 := postSubmitWorkflowWithIdempotency(t, handler, "alpha", "idem-key-1", testIdempotencyWorkflowYAML1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first submit status = %d, want 201 (%s)", rec1.Code, rec1.Body.String())
	}
	var resp1 SubmitWorkflowResponse
	if err := json.Unmarshal(rec1.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("unmarshal first submit response: %v", err)
	}
	if resp1.RunID == "" {
		t.Fatal("expected non-empty RunID on first submit")
	}

	// 2. Second submission with same Idempotency-Key & YAML returns 200 OK with identical run_id.
	rec2 := postSubmitWorkflowWithIdempotency(t, handler, "alpha", "idem-key-1", testIdempotencyWorkflowYAML1)
	if rec2.Code != http.StatusOK {
		t.Fatalf("second submit status = %d, want 200 (%s)", rec2.Code, rec2.Body.String())
	}
	var resp2 SubmitWorkflowResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("unmarshal second submit response: %v", err)
	}
	if resp2.RunID != resp1.RunID {
		t.Fatalf("second submit run_id = %q, want %q", resp2.RunID, resp1.RunID)
	}

	// Verify GET /v1/runs for tenant "alpha" still has only 1 run.
	listReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	listReq.Header.Set(HeaderTenant, "alpha")
	listRec := httptest.NewRecorder()
	handler.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list runs status = %d, want 200 (%s)", listRec.Code, listRec.Body.String())
	}
	var alphaRuns []store.RunRecord
	if err := json.Unmarshal(listRec.Body.Bytes(), &alphaRuns); err != nil {
		t.Fatalf("unmarshal alpha runs: %v", err)
	}
	if len(alphaRuns) != 1 {
		t.Fatalf("alpha runs count = %d, want 1 (%+v)", len(alphaRuns), alphaRuns)
	}

	// 3. Same Idempotency-Key with different YAML returns 409 Conflict.
	rec3 := postSubmitWorkflowWithIdempotency(t, handler, "alpha", "idem-key-1", testIdempotencyWorkflowYAML2)
	if rec3.Code != http.StatusConflict {
		t.Fatalf("conflict submit status = %d, want 409 (%s)", rec3.Code, rec3.Body.String())
	}

	// 4. Same Idempotency-Key under a different tenant ("beta") returns 201 Created with a new run.
	rec4 := postSubmitWorkflowWithIdempotency(t, handler, "beta", "idem-key-1", testIdempotencyWorkflowYAML1)
	if rec4.Code != http.StatusCreated {
		t.Fatalf("beta submit status = %d, want 201 (%s)", rec4.Code, rec4.Body.String())
	}
	var resp4 SubmitWorkflowResponse
	if err := json.Unmarshal(rec4.Body.Bytes(), &resp4); err != nil {
		t.Fatalf("unmarshal beta submit response: %v", err)
	}
	if resp4.RunID == "" || resp4.RunID == resp1.RunID {
		t.Fatalf("beta submit run_id = %q, want non-empty and != %q", resp4.RunID, resp1.RunID)
	}
}
