package chronos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func setupTestClientAndServer(t *testing.T) (*chronos.Client, *runtime.Coordinator, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-test.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   engine.DefaultLeaseTTL,
		ReaperTick: engine.DefaultReaperTick,
	})
	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)

	return chronos.NewClient(srv.URL), coord, srv
}

func postJSON(t *testing.T, url string, body any, out any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("http.Post(%s): %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("http.Post(%s) unexpected status %d", url, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("json.Decode(%s): %v", url, err)
		}
	}
}

func pollAndCompleteTask(t *testing.T, baseURL, workerID, expectedTaskID string, output map[string]any) {
	t.Helper()
	var pollResp runtime.LeasePollResponse
	postJSON(t, baseURL+"/v1/workers/poll", runtime.LeasePollRequest{WorkerID: workerID}, &pollResp)
	if pollResp.Lease == nil {
		t.Fatalf("expected lease for worker %q, got nil", workerID)
	}
	if pollResp.Lease.TaskID != expectedTaskID {
		t.Fatalf("expected task %q, got %q (run %q)", expectedTaskID, pollResp.Lease.TaskID, pollResp.Lease.RunID)
	}
	postJSON(t, baseURL+"/v1/tasks/complete", runtime.TaskCompleteRequest{
		RunID:        pollResp.Lease.RunID,
		TaskID:       pollResp.Lease.TaskID,
		WorkerID:     workerID,
		FencingToken: pollResp.Lease.FencingToken,
		Output:       output,
	}, nil)
}

func TestClientEndToEndLifecycleForkAndMerge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, _, srv := setupTestClientAndServer(t)

	// 1. Health check
	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}

	// 2. SubmitWorkflow + GetRun + Pause/Resume/Cancel lifecycle
	wfYAML := `
name: sdk-e2e
tasks:
  - id: step1
    handler: sim
    checkpoint_name: after_step1
  - id: step2
    handler: sim
    depends_on: [step1]
`
	sub, err := client.SubmitWorkflow(ctx, "run_lifecycle", wfYAML)
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	if sub.RunID != "run_lifecycle" || sub.WorkflowName != "sdk-e2e" || sub.TaskCount != 2 {
		t.Fatalf("unexpected SubmitResponse: %+v", sub)
	}

	snap, err := client.GetRun(ctx, "run_lifecycle")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if snap.State == nil || snap.State.Status != chronos.RunStatusRunning {
		t.Fatalf("expected RUNNING state, got %+v", snap.State)
	}

	paused, err := client.PauseRun(ctx, "run_lifecycle", "maintenance")
	if err != nil {
		t.Fatalf("PauseRun: %v", err)
	}
	if paused.Status != chronos.RunStatusPaused {
		t.Fatalf("expected PAUSED, got %s", paused.Status)
	}

	resumed, err := client.ResumeRun(ctx, "run_lifecycle", "maintenance done")
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	if resumed.Status != chronos.RunStatusRunning {
		t.Fatalf("expected RUNNING, got %s", resumed.Status)
	}

	cancelled, err := client.CancelRun(ctx, "run_lifecycle", "abort")
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if cancelled.Status != chronos.RunStatusCancelled {
		t.Fatalf("expected CANCELLED, got %s", cancelled.Status)
	}

	// Pausing a cancelled run must surface HTTP 409 Conflict.
	_, err = client.PauseRun(ctx, "run_lifecycle", "invalid")
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on PauseRun after Cancel, got %v", err)
	}

	// 3. ForkRun + MergeRun (including HTTP 409 conflict surfacing)
	baseSub, err := client.SubmitWorkflow(ctx, "run_base", wfYAML)
	if err != nil {
		t.Fatalf("SubmitWorkflow(run_base): %v", err)
	}
	if baseSub.RunID != "run_base" {
		t.Fatalf("unexpected base RunID: %s", baseSub.RunID)
	}

	// Complete step1 on run_base so checkpoint "after_step1" exists.
	pollAndCompleteTask(t, srv.URL, "worker-1", "step1", map[string]any{"v": 1})

	// Fork run_branch from checkpoint "after_step1".
	forkResp, err := client.ForkRun(ctx, "run_base", "after_step1", map[string]map[string]any{
		"step2": {"branch_param": "alt"},
	}, "run_branch")
	if err != nil {
		t.Fatalf("ForkRun: %v", err)
	}
	if forkResp.RunID != "run_branch" || forkResp.ParentRunID != "run_base" || forkResp.CheckpointNum != 1 {
		t.Fatalf("unexpected ForkResponse: %+v", forkResp)
	}

	// Complete step2 on both run_base and run_branch to create a merge conflict.
	// Poll order is deterministic by runID: "run_base" < "run_branch".
	pollAndCompleteTask(t, srv.URL, "worker-1", "step2", map[string]any{"from": "base"})
	pollAndCompleteTask(t, srv.URL, "worker-1", "step2", map[string]any{"from": "branch"})

	// Merge with preferBranch=false must fail with HTTP 409 Conflict.
	_, err = client.MergeRun(ctx, "run_base", "run_branch", []string{"step2"}, false, "run_merge_fail")
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on MergeRun, got %v", err)
	}

	// Merge with preferBranch=true must succeed.
	mergeResp, err := client.MergeRun(ctx, "run_base", "run_branch", []string{"step2"}, true, "run_merge_ok")
	if err != nil {
		t.Fatalf("MergeRun(preferBranch=true): %v", err)
	}
	if mergeResp.RunID != "run_merge_ok" || mergeResp.BaseRunID != "run_base" || mergeResp.BranchRunID != "run_branch" {
		t.Fatalf("unexpected MergeResponse: %+v", mergeResp)
	}
	if mergeResp.AdoptedCount != 1 {
		t.Fatalf("expected AdoptedCount=1, got %d", mergeResp.AdoptedCount)
	}
}
