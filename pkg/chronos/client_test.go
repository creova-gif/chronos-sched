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

func TestClientListAndRequeueDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const expectedToken = "secret-dlq-token"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+expectedToken {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dlq":
			allParam := r.URL.Query().Get("all")
			entries := []chronos.DeadLetterEntry{
				{
					ID:           42,
					RunID:        "run-1",
					TaskID:       "task-a",
					WorkflowName: "wf-demo",
					Attempt:      3,
					Reason:       "boom",
					FailedAtNs:   1700000000000,
					Requeued:     false,
				},
			}
			if allParam == "true" {
				entries = append(entries, chronos.DeadLetterEntry{
					ID:            41,
					RunID:         "run-0",
					TaskID:        "task-z",
					WorkflowName:  "wf-demo",
					Attempt:       2,
					Reason:        "timeout",
					FailedAtNs:    1699999999000,
					Requeued:      true,
					RequeuedRunID: "run-0-retry",
				})
			} else if allParam != "" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"unexpected all query param"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(entries)

		case r.Method == http.MethodPost && r.URL.Path == "/v1/dlq/42/requeue":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(chronos.RequeueDeadLetterResponse{
				DeadLetterID:  42,
				RequeuedRunID: "run-1-requeue",
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/dlq/999/requeue":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"dead-letter entry not found"}`))

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"route not found"}`))
		}
	}))
	defer srv.Close()

	client := chronos.NewClient(srv.URL).WithBearerToken(expectedToken)

	// 1. List active DLQ entries (includeRequeued=false -> no ?all=true).
	active, err := client.ListDeadLetters(ctx, false)
	if err != nil {
		t.Fatalf("ListDeadLetters(false): %v", err)
	}
	if len(active) != 1 || active[0].ID != 42 || active[0].Requeued {
		t.Fatalf("unexpected active DLQ entries: %+v", active)
	}

	// 2. List all DLQ entries (includeRequeued=true -> ?all=true).
	all, err := client.ListDeadLetters(ctx, true)
	if err != nil {
		t.Fatalf("ListDeadLetters(true): %v", err)
	}
	if len(all) != 2 || all[1].ID != 41 || !all[1].Requeued || all[1].RequeuedRunID != "run-0-retry" {
		t.Fatalf("unexpected all DLQ entries: %+v", all)
	}

	// 3. Requeue existing DLQ entry.
	requeueResp, err := client.RequeueDeadLetter(ctx, 42)
	if err != nil {
		t.Fatalf("RequeueDeadLetter(42): %v", err)
	}
	if requeueResp.DeadLetterID != 42 || requeueResp.RequeuedRunID != "run-1-requeue" {
		t.Fatalf("unexpected RequeueDeadLetterResponse: %+v", requeueResp)
	}

	// 4. Non-2xx error handling: 404 Not Found.
	_, err = client.RequeueDeadLetter(ctx, 999)
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound || apiErr.Message != "dead-letter entry not found" {
		t.Fatalf("expected 404 APIError, got %v", err)
	}

	// 5. Non-2xx error handling: 401 Unauthorized when bearer token is missing/wrong.
	unauthClient := chronos.NewClient(srv.URL).WithBearerToken("wrong-token")
	_, err = unauthClient.ListDeadLetters(ctx, false)
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 APIError, got %v", err)
	}
}

func TestClientReadyAndDrainWorker(t *testing.T) {
	ctx := context.Background()
	const expectedToken = "drain-test-token"

	readyOK := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/readyz":
			w.Header().Set("Content-Type", "application/json")
			if readyOK {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(chronos.ReadyzStatus{
					Status:     "ready",
					ActiveRuns: 2,
					Workers:    3,
				})
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(chronos.ReadyzStatus{
				Status: "not_ready",
				Error:  "database unavailable",
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/workers/drain":
			if got := r.Header.Get("Authorization"); got != "Bearer "+expectedToken {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid json"}`))
				return
			}
			if body["worker_id"] != "w-1" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"unexpected worker_id"}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(chronos.WorkerDrainResponse{
				WorkerID:      "w-1",
				Draining:      true,
				InFlightTasks: 4,
			})

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"route not found"}`))
		}
	}))
	defer srv.Close()

	client := chronos.NewClient(srv.URL).WithBearerToken(expectedToken)

	// 1. Ready returns 200 ReadyzStatus.
	status, err := client.Ready(ctx)
	if err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	if status.Status != "ready" || status.ActiveRuns != 2 || status.Workers != 3 || status.Error != "" {
		t.Fatalf("unexpected ReadyzStatus: %+v", status)
	}

	// 2. Ready returns *APIError on 503.
	readyOK = false
	_, err = client.Ready(ctx)
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable || apiErr.Message != "database unavailable" {
		t.Fatalf("expected 503 APIError, got %v", err)
	}

	// 3. DrainWorker verifies Bearer token, request body, and decoded response.
	drainResp, err := client.DrainWorker(ctx, "w-1")
	if err != nil {
		t.Fatalf("DrainWorker(w-1) error = %v", err)
	}
	if drainResp.WorkerID != "w-1" || !drainResp.Draining || drainResp.InFlightTasks != 4 {
		t.Fatalf("unexpected WorkerDrainResponse: %+v", drainResp)
	}
}
