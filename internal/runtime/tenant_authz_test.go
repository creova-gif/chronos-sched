package runtime

import (
	"bytes"
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
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	wCtx, wCancel := context.WithCancel(context.Background())
	t.Cleanup(wCancel)
	worker := NewWorker(WorkerConfig{
		WorkerID:       "w-tenant-authz",
		CoordinatorURL: srv.URL,
		Concurrency:    2,
	})
	go func() { _ = worker.Run(wCtx) }()

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

func TestCoordinator_ForkMergeAndDLQTenantIsolation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tenant-lineage-dlq.db")
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
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	wCtx, wCancel := context.WithCancel(context.Background())
	t.Cleanup(wCancel)
	worker := NewWorker(WorkerConfig{
		WorkerID:       "w-tenant-authz",
		CoordinatorURL: srv.URL,
		Concurrency:    2,
	})
	go func() { _ = worker.Run(wCtx) }()

	doReq := func(method, path, tenant string, payload any) *httptest.ResponseRecorder {
		t.Helper()
		var body *bytes.Reader
		if payload != nil {
			b, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal %s %s: %v", method, path, err)
			}
			body = bytes.NewReader(b)
		} else {
			body = bytes.NewReader(nil)
		}
		req := httptest.NewRequest(method, path, body)
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set(HeaderTenant, tenant)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Submit runs for alpha and beta.
	alphaSubRec := doReq(http.MethodPost, "/v1/workflows/run", "alpha", SubmitWorkflowRequest{
		WorkflowYAML: testTenantIsolationWorkflowYAML,
	})
	if alphaSubRec.Code != http.StatusCreated {
		t.Fatalf("alpha submit status = %d, want 201 (%s)", alphaSubRec.Code, alphaSubRec.Body.String())
	}
	var alphaSub SubmitWorkflowResponse
	if err := json.Unmarshal(alphaSubRec.Body.Bytes(), &alphaSub); err != nil {
		t.Fatalf("unmarshal alpha submit: %v", err)
	}

	betaSubRec := doReq(http.MethodPost, "/v1/workflows/run", "beta", SubmitWorkflowRequest{
		WorkflowYAML: testTenantIsolationWorkflowYAML,
	})
	if betaSubRec.Code != http.StatusCreated {
		t.Fatalf("beta submit status = %d, want 201 (%s)", betaSubRec.Code, betaSubRec.Body.String())
	}
	var betaSub SubmitWorkflowResponse
	if err := json.Unmarshal(betaSubRec.Body.Bytes(), &betaSub); err != nil {
		t.Fatalf("unmarshal beta submit: %v", err)
	}

	// Wait for alphaSub to complete step1 so checkpoint 1 exists for forking.
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := coord.Snapshot(context.Background(), alphaSub.RunID)
		if err == nil && len(snap.State.Checkpoints) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for checkpoint on %s", alphaSub.RunID)
		}
		time.Sleep(15 * time.Millisecond)
	}

	// 1. Forking under alpha assigns the forked run to alpha (alpha sees it, beta gets 404).
	forkRec := doReq(http.MethodPost, "/v1/runs/"+alphaSub.RunID+"/fork", "alpha", ForkRunRequest{
		Checkpoint: "1",
	})
	if forkRec.Code != http.StatusCreated {
		t.Fatalf("alpha fork status = %d, want 201 (%s)", forkRec.Code, forkRec.Body.String())
	}
	var forkResp ForkRunResponse
	if err := json.Unmarshal(forkRec.Body.Bytes(), &forkResp); err != nil {
		t.Fatalf("unmarshal fork response: %v", err)
	}
	if forkResp.RunID == "" {
		t.Fatal("expected non-empty forked RunID")
	}

	if getAlphaFork := doReq(http.MethodGet, "/v1/runs/"+forkResp.RunID, "alpha", nil); getAlphaFork.Code != http.StatusOK {
		t.Fatalf("alpha GET forked run status = %d, want 200 (%s)", getAlphaFork.Code, getAlphaFork.Body.String())
	}
	if getBetaFork := doReq(http.MethodGet, "/v1/runs/"+forkResp.RunID, "beta", nil); getBetaFork.Code != http.StatusNotFound {
		t.Fatalf("beta GET forked run status = %d, want 404 (%s)", getBetaFork.Code, getBetaFork.Body.String())
	}

	// 2. Merging alpha's run with beta's run under alpha returns 404.
	mergeRec := doReq(http.MethodPost, "/v1/runs/"+alphaSub.RunID+"/merge", "alpha", MergeRunRequest{
		BranchRunID: betaSub.RunID,
	})
	if mergeRec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant merge status = %d, want 404 (%s)", mergeRec.Code, mergeRec.Body.String())
	}

	// 3. Dead-letter queue isolation.
	if err := st.RecordDeadLetter(context.Background(), store.DeadLetterEntry{
		RunID:        alphaSub.RunID,
		TaskID:       "step1",
		WorkflowName: alphaSub.WorkflowName,
		Attempt:      3,
		Reason:       "max retries exhausted",
		FailedAtNs:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	// Beta sees empty DLQ list.
	betaListRec := doReq(http.MethodGet, "/v1/dlq", "beta", nil)
	if betaListRec.Code != http.StatusOK {
		t.Fatalf("beta GET /v1/dlq status = %d, want 200 (%s)", betaListRec.Code, betaListRec.Body.String())
	}
	var betaDLQ ListDeadLettersResponse
	if err := json.Unmarshal(betaListRec.Body.Bytes(), &betaDLQ); err != nil {
		t.Fatalf("unmarshal beta DLQ list: %v", err)
	}
	if betaDLQ.Entries == nil || len(betaDLQ.Entries) != 0 {
		t.Fatalf("beta DLQ entries = %#v, want empty non-nil slice", betaDLQ.Entries)
	}

	// Alpha sees its DLQ entry.
	alphaListRec := doReq(http.MethodGet, "/v1/dlq", "alpha", nil)
	if alphaListRec.Code != http.StatusOK {
		t.Fatalf("alpha GET /v1/dlq status = %d, want 200 (%s)", alphaListRec.Code, alphaListRec.Body.String())
	}
	var alphaDLQ ListDeadLettersResponse
	if err := json.Unmarshal(alphaListRec.Body.Bytes(), &alphaDLQ); err != nil {
		t.Fatalf("unmarshal alpha DLQ list: %v", err)
	}
	if len(alphaDLQ.Entries) != 1 || alphaDLQ.Entries[0].RunID != alphaSub.RunID {
		t.Fatalf("alpha DLQ entries = %#v, want 1 entry for %s", alphaDLQ.Entries, alphaSub.RunID)
	}
	dlqID := alphaDLQ.Entries[0].ID

	// Beta cannot requeue alpha's DLQ entry (404).
	betaRequeueRec := doReq(http.MethodPost, fmt.Sprintf("/v1/dlq/%d/requeue", dlqID), "beta", nil)
	if betaRequeueRec.Code != http.StatusNotFound {
		t.Fatalf("beta requeue status = %d, want 404 (%s)", betaRequeueRec.Code, betaRequeueRec.Body.String())
	}

	// Alpha requeues its DLQ entry cleanly and owns the requeued run.
	alphaRequeueRec := doReq(http.MethodPost, fmt.Sprintf("/v1/dlq/%d/requeue", dlqID), "alpha", nil)
	if alphaRequeueRec.Code != http.StatusCreated {
		t.Fatalf("alpha requeue status = %d, want 201 (%s)", alphaRequeueRec.Code, alphaRequeueRec.Body.String())
	}
	var requeueResp RequeueDeadLetterResponse
	if err := json.Unmarshal(alphaRequeueRec.Body.Bytes(), &requeueResp); err != nil {
		t.Fatalf("unmarshal requeue response: %v", err)
	}
	if requeueResp.RequeuedRunID == "" {
		t.Fatal("expected non-empty RequeuedRunID")
	}
	if getAlphaRequeue := doReq(http.MethodGet, "/v1/runs/"+requeueResp.RequeuedRunID, "alpha", nil); getAlphaRequeue.Code != http.StatusOK {
		t.Fatalf("alpha GET requeued run status = %d, want 200 (%s)", getAlphaRequeue.Code, getAlphaRequeue.Body.String())
	}
	if getBetaRequeue := doReq(http.MethodGet, "/v1/runs/"+requeueResp.RequeuedRunID, "beta", nil); getBetaRequeue.Code != http.StatusNotFound {
		t.Fatalf("beta GET requeued run status = %d, want 404 (%s)", getBetaRequeue.Code, getBetaRequeue.Body.String())
	}
}
