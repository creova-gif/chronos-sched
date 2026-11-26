package runtime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestCoordinator_TenantActiveRunQuota(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-quota.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:               engine.DefaultLeaseTTL,
		ReaperTick:             engine.DefaultReaperTick,
		MaxActiveRunsPerTenant: 1,
	})
	handler := coord.Handler()

	submitForTenant := func(tenant string) *httptest.ResponseRecorder {
		body, err := json.Marshal(SubmitWorkflowRequest{
			WorkflowYAML: testTenantIsolationWorkflowYAML,
		})
		if err != nil {
			t.Fatalf("marshal submit request: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(HeaderTenant, tenant)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// 1. First submit for alpha succeeds with 201.
	alphaRec1 := submitForTenant("alpha")
	if alphaRec1.Code != http.StatusCreated {
		t.Fatalf("alpha 1st submit status = %d, want 201 (%s)", alphaRec1.Code, alphaRec1.Body.String())
	}
	var alphaResp1 SubmitWorkflowResponse
	if err := json.Unmarshal(alphaRec1.Body.Bytes(), &alphaResp1); err != nil {
		t.Fatalf("unmarshal alpha 1st submit response: %v", err)
	}

	// 2. Second submit for alpha is rejected with 429.
	alphaRec2 := submitForTenant("alpha")
	if alphaRec2.Code != http.StatusTooManyRequests {
		t.Fatalf("alpha 2nd submit status = %d, want 429 (%s)", alphaRec2.Code, alphaRec2.Body.String())
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(alphaRec2.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal 429 ErrorResponse: %v", err)
	}
	if errResp.Error != ErrTenantQuotaExceeded.Error() {
		t.Fatalf("ErrorResponse.Error = %q, want %q", errResp.Error, ErrTenantQuotaExceeded.Error())
	}

	// 3. Fork for alpha is also rejected with 429.
	forkBody, err := json.Marshal(ForkRunRequest{Checkpoint: "0"})
	if err != nil {
		t.Fatalf("marshal fork request: %v", err)
	}
	forkReq := httptest.NewRequest(http.MethodPost, "/v1/runs/"+alphaResp1.RunID+"/fork", bytes.NewReader(forkBody))
	forkReq.Header.Set("Content-Type", "application/json")
	forkReq.Header.Set(HeaderTenant, "alpha")
	forkRec := httptest.NewRecorder()
	handler.ServeHTTP(forkRec, forkReq)
	if forkRec.Code != http.StatusTooManyRequests {
		t.Fatalf("alpha fork status = %d, want 429 (%s)", forkRec.Code, forkRec.Body.String())
	}

	// 4. Beta can submit independently and succeeds with 201.
	betaRec := submitForTenant("beta")
	if betaRec.Code != http.StatusCreated {
		t.Fatalf("beta submit status = %d, want 201 (%s)", betaRec.Code, betaRec.Body.String())
	}

	// 5. Cancel alpha's active run, then alpha can submit again (201).
	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/runs/"+alphaResp1.RunID+"/cancel", nil)
	cancelReq.Header.Set(HeaderTenant, "alpha")
	cancelRec := httptest.NewRecorder()
	handler.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("alpha cancel status = %d, want 200 (%s)", cancelRec.Code, cancelRec.Body.String())
	}

	alphaRec3 := submitForTenant("alpha")
	if alphaRec3.Code != http.StatusCreated {
		t.Fatalf("alpha submit after cancel status = %d, want 201 (%s)", alphaRec3.Code, alphaRec3.Body.String())
	}
}
