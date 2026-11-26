package runtime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
	"github.com/creova-gif/chronos-sched/pkg/chronos"
)

func TestE2E_ApprovalGateHTTPToReplayAndTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Open SQLite WAL store in t.TempDir(), start Coordinator with AuthToken, serve via httptest.NewServer.
	dbPath := filepath.Join(t.TempDir(), "approval_e2e.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const (
		authToken = "m4-tok"
		tenantID  = "acme-prod"
	)
	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   2 * time.Second,
		ReaperTick: 50 * time.Millisecond,
		AuthToken:  authToken,
	})
	go coord.StartReaper(ctx)

	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	// 2. Create typed chronos.Client with Bearer token and Tenant header.
	client := chronos.NewClient(srv.URL).WithBearerToken(authToken).WithTenant(tenantID)

	// 3. Submit a 1-task workflow (task1), immediately call RequestTaskApproval,
	// verify TaskStatus == "WAITING_APPROVAL", and verify X-Chronos-Tenant: acme-prod is echoed.
	wfYAML := `name: approval-e2e-wf
tasks:
  - id: task1
    handler: approval_ok
    max_retries: 0
`
	sub, err := client.SubmitWorkflow(ctx, "", wfYAML)
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	if sub.RunID == "" {
		t.Fatal("SubmitWorkflow returned empty RunID")
	}

	reqApprovalResp, err := client.RequestTaskApproval(ctx, sub.RunID, "task1", "secops", "awaiting change ticket")
	if err != nil {
		t.Fatalf("RequestTaskApproval: %v", err)
	}
	if reqApprovalResp.TaskStatus != "WAITING_APPROVAL" {
		t.Fatalf("RequestTaskApproval TaskStatus = %q, want %q", reqApprovalResp.TaskStatus, "WAITING_APPROVAL")
	}

	// Verify X-Chronos-Tenant: acme-prod is echoed by the Coordinator HTTP middleware.
	tenantReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/runs/"+sub.RunID, nil)
	if err != nil {
		t.Fatalf("new tenant check request: %v", err)
	}
	tenantReq.Header.Set("Authorization", "Bearer "+authToken)
	tenantReq.Header.Set(runtime.HeaderTenant, tenantID)
	tenantResp, err := (&http.Client{Timeout: 5 * time.Second}).Do(tenantReq)
	if err != nil {
		t.Fatalf("GET /v1/runs/%s: %v", sub.RunID, err)
	}
	_ = tenantResp.Body.Close()
	if tenantResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/runs/%s status = %d, want 200", sub.RunID, tenantResp.StatusCode)
	}
	if gotTenant := tenantResp.Header.Get(runtime.HeaderTenant); gotTenant != tenantID {
		t.Fatalf("echoed %s = %q, want %q", runtime.HeaderTenant, gotTenant, tenantID)
	}

	// 4. Start a Worker with AuthToken: "m4-tok" and a deterministic handler that succeeds on task1.
	worker := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "worker-approval-e2e",
		CoordinatorURL:    srv.URL,
		AuthToken:         authToken,
		Concurrency:       1,
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	})
	worker.RegisterHandler("approval_ok", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		return map[string]any{"approved_exec": true}, nil
	})
	go func() {
		_ = worker.Run(ctx)
	}()

	// 5. Resolve approval (approved: true, actor: "alice", comment: "approved CHG-42") -> verify TaskStatus == "PENDING".
	resolveResp, err := client.ResolveTaskApproval(ctx, sub.RunID, "task1", "alice", "approved CHG-42", true)
	if err != nil {
		t.Fatalf("ResolveTaskApproval: %v", err)
	}
	if resolveResp.TaskStatus != "PENDING" {
		t.Fatalf("ResolveTaskApproval TaskStatus = %q, want %q", resolveResp.TaskStatus, "PENDING")
	}

	// 6. Wait for the run to reach COMPLETED via client.GetRun.
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, err := client.GetRun(ctx, sub.RunID)
		if err != nil {
			t.Fatalf("GetRun(%s): %v", sub.RunID, err)
		}
		if snap.State != nil && snap.State.Status == chronos.RunStatusCompleted {
			break
		}
		if snap.State != nil && snap.State.Status == chronos.RunStatusFailed {
			t.Fatalf("run %s reached FAILED: %s", sub.RunID, snap.State.FailureReason)
		}
		if time.Now().After(deadline) {
			var gotStatus chronos.RunStatus
			if snap.State != nil {
				gotStatus = snap.State.Status
			}
			t.Fatalf("timed out waiting for run %s to reach COMPLETED (last status=%q)", sub.RunID, gotStatus)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// 7. Load events from SQLite and run engine.VerifyReplay(dag, evs).
	evs, err := st.LoadRunEvents(ctx, sub.RunID)
	if err != nil {
		t.Fatalf("LoadRunEvents: %v", err)
	}

	var sawApprovalRequested, sawApproved bool
	for _, ev := range evs {
		switch string(ev.Type) {
		case "TASK_APPROVAL_REQUESTED":
			sawApprovalRequested = true
		case "TASK_APPROVED":
			sawApproved = true
		}
	}
	if !sawApprovalRequested {
		t.Fatalf("expected TASK_APPROVAL_REQUESTED event in journal, got %d events", len(evs))
	}
	if !sawApproved {
		t.Fatalf("expected TASK_APPROVED event in journal, got %d events", len(evs))
	}

	dag, err := engine.ParseDAG([]byte(wfYAML))
	if err != nil {
		t.Fatalf("engine.ParseDAG: %v", err)
	}
	rep, err := engine.VerifyReplay(dag, evs)
	if err != nil {
		t.Fatalf("engine.VerifyReplay: %v", err)
	}
	if rep.FinalStatus != engine.RunStatusCompleted || rep.CompletedTasks != 1 {
		t.Fatalf("unexpected replay report: %+v (want FinalStatus=COMPLETED, CompletedTasks=1)", rep)
	}
}
