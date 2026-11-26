package runtime_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestWorkerHandlerPanicRecoveryAndContinuation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "worker_panic.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const authToken = "panic-test-tok"
	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   2 * time.Second,
		ReaperTick: 50 * time.Millisecond,
		AuthToken:  authToken,
	})
	go coord.StartReaper(ctx)

	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	worker := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "worker-panic-test",
		CoordinatorURL:    srv.URL,
		AuthToken:         authToken,
		Concurrency:       2,
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	})
	worker.RegisterHandler("panic_handler", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		panic("boom")
	})
	worker.RegisterHandler("ok", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		return map[string]any{"ok": true}, nil
	})

	workerErrCh := make(chan error, 1)
	go func() {
		workerErrCh <- worker.Run(workerCtx)
	}()

	// 1. Submit first workflow whose handler panics with max_retries: 0.
	panicWF := `name: panic-wf
tasks:
  - id: task1
    handler: panic_handler
    max_retries: 0
`
	run1 := submitWorkflowForPanicTest(t, ctx, client, srv.URL, authToken, panicWF)
	waitForRunStatus(t, ctx, client, srv.URL, authToken, run1.RunID, engine.RunStatusFailed)

	// Verify run state records the panic error.
	snap1 := getRunSnapshotForPanicTest(t, ctx, client, srv.URL, authToken, run1.RunID)
	if snap1.State.Status != engine.RunStatusFailed {
		t.Fatalf("run1 status = %s, want %s", snap1.State.Status, engine.RunStatusFailed)
	}
	task1 := snap1.State.Tasks["task1"]
	if task1 == nil {
		t.Fatalf("run1 missing task1 in state")
	}
	if !strings.Contains(task1.LastError, "handler panic: boom") && !strings.Contains(snap1.State.FailureReason, "handler panic: boom") {
		t.Fatalf("expected 'handler panic: boom' in task1.LastError (%q) or FailureReason (%q)", task1.LastError, snap1.State.FailureReason)
	}

	// Verify dead-letter entry is recorded.
	dlqResp := getDLQ(t, ctx, client, srv.URL+"/v1/dlq", authToken)
	if len(dlqResp.Entries) != 1 {
		t.Fatalf("len(DLQ entries) = %d, want 1 (%+v)", len(dlqResp.Entries), dlqResp.Entries)
	}
	entry := dlqResp.Entries[0]
	if entry.RunID != run1.RunID || entry.TaskID != "task1" || !strings.Contains(entry.Reason, "handler panic: boom") {
		t.Fatalf("unexpected DLQ entry: %+v", entry)
	}

	// Verify the worker did NOT crash or exit.
	select {
	case err := <-workerErrCh:
		t.Fatalf("worker exited unexpectedly after handler panic: %v", err)
	default:
	}

	// 2. Submit second workflow with a normal "ok" handler on the SAME live Worker.
	okWF := `name: ok-wf
tasks:
  - id: task1
    handler: ok
    max_retries: 0
`
	run2 := submitWorkflowForPanicTest(t, ctx, client, srv.URL, authToken, okWF)
	waitForRunStatus(t, ctx, client, srv.URL, authToken, run2.RunID, engine.RunStatusCompleted)

	workerCancel()
	select {
	case err := <-workerErrCh:
		if err != nil {
			t.Fatalf("worker.Run returned error on shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker.Run to exit")
	}
}

func submitWorkflowForPanicTest(t *testing.T, ctx context.Context, client *http.Client, baseURL, token, wfYAML string) runtime.SubmitWorkflowResponse {
	t.Helper()
	body, err := json.Marshal(runtime.SubmitWorkflowRequest{WorkflowYAML: wfYAML})
	if err != nil {
		t.Fatalf("marshal submit request: %v", err)
	}
	resp, err := doAuthJSON(ctx, client, http.MethodPost, baseURL+"/v1/workflows/run", token, body)
	if err != nil {
		t.Fatalf("POST /v1/workflows/run: %v", err)
	}
	respBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/workflows/run status = %d, body = %s", resp.StatusCode, string(respBytes))
	}
	var out runtime.SubmitWorkflowResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		t.Fatalf("unmarshal submit response: %v", err)
	}
	return out
}

func getRunSnapshotForPanicTest(t *testing.T, ctx context.Context, client *http.Client, baseURL, token, runID string) runtime.RunSnapshotResponse {
	t.Helper()
	resp, err := doAuthJSON(ctx, client, http.MethodGet, baseURL+"/v1/runs/"+runID, token, nil)
	if err != nil {
		t.Fatalf("GET /v1/runs/%s: %v", runID, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/runs/%s status = %d: %s", runID, resp.StatusCode, string(body))
	}
	var snap runtime.RunSnapshotResponse
	if err := json.Unmarshal(body, &snap); err != nil || snap.State == nil {
		t.Fatalf("unmarshal run snapshot: err=%v, state=%v", err, snap.State)
	}
	return snap
}
