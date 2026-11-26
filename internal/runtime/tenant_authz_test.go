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
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestCoordinator_AuthorizeRunTenantHelper(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-authz.db")
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

	var resp SubmitWorkflowResponse
	if err := json.Unmarshal(submitRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal submit response: %v", err)
	}
	if resp.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}

	alphaCtx := WithTenant(context.Background(), "alpha")

	// Owning tenant "alpha" is authorized.
	alphaReq := httptest.NewRequest(http.MethodGet, "/v1/runs/"+resp.RunID, nil).WithContext(alphaCtx)
	alphaRec := httptest.NewRecorder()
	if !coord.authorizeRunTenant(alphaRec, alphaReq, resp.RunID) {
		t.Fatalf("authorizeRunTenant(alpha) = false, want true (status=%d body=%s)", alphaRec.Code, alphaRec.Body.String())
	}

	// Cross-tenant "beta" is rejected with 404.
	betaCtx := WithTenant(context.Background(), "beta")
	betaReq := httptest.NewRequest(http.MethodGet, "/v1/runs/"+resp.RunID, nil).WithContext(betaCtx)
	betaRec := httptest.NewRecorder()
	if coord.authorizeRunTenant(betaRec, betaReq, resp.RunID) {
		t.Fatal("authorizeRunTenant(beta) = true, want false")
	}
	if betaRec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (%s)", betaRec.Code, betaRec.Body.String())
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(betaRec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal ErrorResponse: %v", err)
	}
	if errResp.Error != store.ErrRunNotFound.Error() {
		t.Fatalf("ErrorResponse.Error = %q, want %q", errResp.Error, store.ErrRunNotFound.Error())
	}
}
