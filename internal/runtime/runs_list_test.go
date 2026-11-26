package runtime

import (
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

func TestCoordinator_ListRunsFilterAndLimit(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "runs-list.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	seedRuns := []struct {
		id     string
		tenant string
		status engine.RunStatus
		ts     int64
	}{
		{id: "alpha-run-1", tenant: "alpha", status: engine.RunStatusRunning, ts: 100},
		{id: "alpha-run-2", tenant: "alpha", status: engine.RunStatusCompleted, ts: 200},
		{id: "alpha-run-3", tenant: "alpha", status: engine.RunStatusCompleted, ts: 300},
		{id: "alpha-run-4", tenant: "alpha", status: engine.RunStatusFailed, ts: 400},
		{id: "beta-run-1", tenant: "beta", status: engine.RunStatusCompleted, ts: 500},
		{id: "beta-run-2", tenant: "beta", status: engine.RunStatusRunning, ts: 600},
	}

	for _, r := range seedRuns {
		rec := store.RunRecord{
			RunID:        r.id,
			WorkflowName: "test-wf",
			WorkflowHash: "hash-" + r.id,
			WorkflowYAML: "name: test-wf\ntasks:\n  - id: a\n    handler: sim\n",
			Status:       r.status,
			CreatedAtNs:  r.ts,
		}
		if err := st.CreateRun(ctx, rec, nil); err != nil {
			t.Fatalf("CreateRun(%s): %v", r.id, err)
		}
		if err := st.SetRunTenant(ctx, r.id, r.tenant); err != nil {
			t.Fatalf("SetRunTenant(%s): %v", r.id, err)
		}
	}

	coord := NewCoordinator(st, CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	handler := coord.Handler()

	doList := func(tenant, query string) *httptest.ResponseRecorder {
		target := "/v1/runs"
		if query != "" {
			target += "?" + query
		}
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set(HeaderTenant, tenant)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Filter ?status=COMPLETED for alpha returns only alpha's 2 completed runs.
	rec := doList("alpha", "status=COMPLETED")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/runs?status=COMPLETED status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var alphaCompleted []store.RunRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &alphaCompleted); err != nil {
		t.Fatalf("unmarshal alpha completed: %v", err)
	}
	if len(alphaCompleted) != 2 {
		t.Fatalf("alpha completed len = %d, want 2 (%+v)", len(alphaCompleted), alphaCompleted)
	}
	for _, run := range alphaCompleted {
		if run.Status != engine.RunStatusCompleted {
			t.Fatalf("unexpected run status %s, want COMPLETED", run.Status)
		}
		if run.RunID == "beta-run-1" {
			t.Fatalf("beta run leaked into alpha results")
		}
	}

	// Limit ?limit=2 for alpha returns 2 of alpha's 4 runs.
	rec = doList("alpha", "limit=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/runs?limit=2 status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var alphaLimited []store.RunRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &alphaLimited); err != nil {
		t.Fatalf("unmarshal alpha limited: %v", err)
	}
	if len(alphaLimited) != 2 {
		t.Fatalf("alpha limited len = %d, want 2", len(alphaLimited))
	}

	// Tenant-scoped limiting: beta with ?limit=2 returns only beta's 2 runs, never alpha's.
	rec = doList("beta", "limit=2")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/runs?limit=2 (beta) status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var betaLimited []store.RunRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &betaLimited); err != nil {
		t.Fatalf("unmarshal beta limited: %v", err)
	}
	if len(betaLimited) != 2 {
		t.Fatalf("beta limited len = %d, want 2", len(betaLimited))
	}
	for _, run := range betaLimited {
		if run.RunID != "beta-run-1" && run.RunID != "beta-run-2" {
			t.Fatalf("unexpected run %q in beta results", run.RunID)
		}
	}

	// Beta with ?status=FAILED returns empty JSON array [] (not null).
	rec = doList("beta", "status=FAILED")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/runs?status=FAILED (beta) status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "[]\n" && got != "[]" {
		// Also verify unmarshaling into slice gives non-nil or 0 length.
		var empty []store.RunRecord
		if err := json.Unmarshal(rec.Body.Bytes(), &empty); err != nil || len(empty) != 0 {
			t.Fatalf("expected empty array, got %q", got)
		}
	}

	// Invalid queries return 400.
	invalidQueries := []string{
		"limit=0",
		"limit=-1",
		"limit=501",
		"limit=abc",
		"limit=",
		"status=BOGUS",
		"status=COMPLETED,BOGUS",
	}
	for _, q := range invalidQueries {
		rec := doList("alpha", q)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET /v1/runs?%s status = %d, want 400 (%s)", q, rec.Code, rec.Body.String())
		}
	}
}
