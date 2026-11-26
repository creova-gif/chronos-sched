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

const testTenantIsolationWorkflowYAML = `name: tenant-isolation-wf
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 10
`

func TestCoordinator_RowLevelTenantIsolation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-isolation.db")
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

	// Submit workflow as tenant "alpha".
	body, err := json.Marshal(SubmitWorkflowRequest{
		WorkflowYAML: testTenantIsolationWorkflowYAML,
	})
	if err != nil {
		t.Fatalf("marshal submit request: %v", err)
	}
	submitReq := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", bytes.NewReader(body))
	submitReq.Header.Set("Content-Type", "application/json")
	submitReq.Header.Set(HeaderTenant, "alpha")
	submitRec := httptest.NewRecorder()
	handler.ServeHTTP(submitRec, submitReq)
	if submitRec.Code != http.StatusCreated {
		t.Fatalf("submit status = %d, want 201 (%s)", submitRec.Code, submitRec.Body.String())
	}

	var submitResp SubmitWorkflowResponse
	if err := json.Unmarshal(submitRec.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("unmarshal submit response: %v", err)
	}
	if submitResp.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}

	// GET /v1/runs as "alpha" returns 1 run.
	alphaListReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	alphaListReq.Header.Set(HeaderTenant, "alpha")
	alphaListRec := httptest.NewRecorder()
	handler.ServeHTTP(alphaListRec, alphaListReq)
	if alphaListRec.Code != http.StatusOK {
		t.Fatalf("alpha list status = %d, want 200 (%s)", alphaListRec.Code, alphaListRec.Body.String())
	}
	var alphaRuns []store.RunRecord
	if err := json.Unmarshal(alphaListRec.Body.Bytes(), &alphaRuns); err != nil {
		t.Fatalf("unmarshal alpha runs: %v", err)
	}
	if len(alphaRuns) != 1 || alphaRuns[0].RunID != submitResp.RunID {
		t.Fatalf("alpha runs = %+v, want 1 run with ID %q", alphaRuns, submitResp.RunID)
	}

	// GET /v1/runs as "beta" returns 0 runs.
	betaListReq := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
	betaListReq.Header.Set(HeaderTenant, "beta")
	betaListRec := httptest.NewRecorder()
	handler.ServeHTTP(betaListRec, betaListReq)
	if betaListRec.Code != http.StatusOK {
		t.Fatalf("beta list status = %d, want 200 (%s)", betaListRec.Code, betaListRec.Body.String())
	}
	var betaRuns []store.RunRecord
	if err := json.Unmarshal(betaListRec.Body.Bytes(), &betaRuns); err != nil {
		t.Fatalf("unmarshal beta runs: %v", err)
	}
	if len(betaRuns) != 0 {
		t.Fatalf("beta runs = %+v, want 0 runs", betaRuns)
	}

	// GET /v1/runs/{id} as "alpha" returns 200.
	alphaGetReq := httptest.NewRequest(http.MethodGet, "/v1/runs/"+submitResp.RunID, nil)
	alphaGetReq.Header.Set(HeaderTenant, "alpha")
	alphaGetRec := httptest.NewRecorder()
	handler.ServeHTTP(alphaGetRec, alphaGetReq)
	if alphaGetRec.Code != http.StatusOK {
		t.Fatalf("alpha get run status = %d, want 200 (%s)", alphaGetRec.Code, alphaGetRec.Body.String())
	}

	// GET /v1/runs/{id} as "beta" returns 404.
	betaGetReq := httptest.NewRequest(http.MethodGet, "/v1/runs/"+submitResp.RunID, nil)
	betaGetReq.Header.Set(HeaderTenant, "beta")
	betaGetRec := httptest.NewRecorder()
	handler.ServeHTTP(betaGetRec, betaGetReq)
	if betaGetRec.Code != http.StatusNotFound {
		t.Fatalf("beta get run status = %d, want 404 (%s)", betaGetRec.Code, betaGetRec.Body.String())
	}
}
