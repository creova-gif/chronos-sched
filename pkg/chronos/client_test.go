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

func TestClientListWorkers(t *testing.T) {
	ctx := context.Background()
	const expectedToken = "secret-workers-token"

	var statusCode int = http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/workers" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"route not found"}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+expectedToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch statusCode {
		case http.StatusUnauthorized:
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid token"}`))
		case http.StatusInternalServerError:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal error"}`))
		default:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"workers": []chronos.WorkerInfo{
					{
						WorkerID:      "w-1",
						PID:           1001,
						Concurrency:   4,
						Resources:     map[string]int{"gpu": 1},
						LastHeartbeat: 1700000000,
						Alive:         true,
						Draining:      false,
					},
					{
						WorkerID:      "w-2",
						PID:           1002,
						Concurrency:   2,
						LastHeartbeat: 1699999000,
						Alive:         false,
						Draining:      true,
					},
				},
			})
		}
	}))
	defer srv.Close()

	client := chronos.NewClient(srv.URL).WithBearerToken(expectedToken)

	// 1. 200 OK returns decoded []WorkerInfo.
	workers, err := client.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("expected 2 workers, got %d", len(workers))
	}
	if workers[0].WorkerID != "w-1" || workers[0].PID != 1001 || workers[0].Concurrency != 4 ||
		workers[0].Resources["gpu"] != 1 || workers[0].LastHeartbeat != 1700000000 ||
		!workers[0].Alive || workers[0].Draining {
		t.Fatalf("unexpected workers[0]: %+v", workers[0])
	}
	if workers[1].WorkerID != "w-2" || workers[1].PID != 1002 || workers[1].Concurrency != 2 ||
		workers[1].LastHeartbeat != 1699999000 || workers[1].Alive || !workers[1].Draining {
		t.Fatalf("unexpected workers[1]: %+v", workers[1])
	}

	// 2. 401 Unauthorized returns *chronos.APIError.
	statusCode = http.StatusUnauthorized
	_, err = client.ListWorkers(ctx)
	var apiErr *chronos.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized || apiErr.Message != "invalid token" {
		t.Fatalf("expected 401 APIError, got %v", err)
	}

	// 3. 500 Internal Server Error returns *chronos.APIError.
	statusCode = http.StatusInternalServerError
	_, err = client.ListWorkers(ctx)
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError || apiErr.Message != "internal error" {
		t.Fatalf("expected 500 APIError, got %v", err)
	}

	// 4. Missing/wrong Bearer token triggers 401 *chronos.APIError.
	statusCode = http.StatusOK
	unauthClient := chronos.NewClient(srv.URL).WithBearerToken("wrong-token")
	_, err = unauthClient.ListWorkers(ctx)
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized || apiErr.Message != "unauthorized" {
		t.Fatalf("expected 401 APIError for wrong token, got %v", err)
	}
}

func TestClientWithTenantHeader(t *testing.T) {
	ctx := context.Background()

	t.Run("doJSON", func(t *testing.T) {
		var gotTenant string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTenant = r.Header.Get("X-Chronos-Tenant")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL).WithTenant("acme-prod")
		if err := client.Health(ctx); err != nil {
			t.Fatalf("Health() error = %v", err)
		}
		if gotTenant != "acme-prod" {
			t.Fatalf("expected X-Chronos-Tenant header %q, got %q", "acme-prod", gotTenant)
		}
	})

	t.Run("GetArtifact", func(t *testing.T) {
		var gotTenant string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTenant = r.Header.Get("X-Chronos-Tenant")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("artifact-bytes"))
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL).WithTenant("acme-prod")
		data, err := client.GetArtifact(ctx, "run-1", "task-1", "output.log")
		if err != nil {
			t.Fatalf("GetArtifact() error = %v", err)
		}
		if string(data) != "artifact-bytes" {
			t.Fatalf("unexpected artifact body %q", string(data))
		}
		if gotTenant != "acme-prod" {
			t.Fatalf("expected X-Chronos-Tenant header %q, got %q", "acme-prod", gotTenant)
		}
	})

	t.Run("StreamRunEvents", func(t *testing.T) {
		var gotTenant string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTenant = r.Header.Get("X-Chronos-Tenant")
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL).WithTenant("acme-prod")
		err := client.StreamRunEvents(ctx, "run-1", func(chronos.StreamEvent) error {
			return nil
		})
		if err != nil {
			t.Fatalf("StreamRunEvents() error = %v", err)
		}
		if gotTenant != "acme-prod" {
			t.Fatalf("expected X-Chronos-Tenant header %q, got %q", "acme-prod", gotTenant)
		}
	})
}

func TestClientSubmitWorkflowIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("201 Created with Idempotency-Key", func(t *testing.T) {
		var gotKey string
		var gotMethod, gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotPath = r.URL.Path
			gotKey = r.Header.Get("Idempotency-Key")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(chronos.SubmitResponse{
				RunID:        "run-idem-1",
				WorkflowName: "demo",
				TaskCount:    2,
			})
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL)
		resp, err := client.SubmitWorkflowIdempotent(ctx, "run-idem-1", "name: demo", "key-123")
		if err != nil {
			t.Fatalf("SubmitWorkflowIdempotent() error = %v", err)
		}
		if gotMethod != http.MethodPost || gotPath != "/v1/workflows/run" {
			t.Fatalf("unexpected request %s %s", gotMethod, gotPath)
		}
		if gotKey != "key-123" {
			t.Fatalf("expected Idempotency-Key %q, got %q", "key-123", gotKey)
		}
		if resp.RunID != "run-idem-1" || resp.WorkflowName != "demo" || resp.TaskCount != 2 {
			t.Fatalf("unexpected SubmitResponse: %+v", resp)
		}
	})

	t.Run("200 OK replay decoding", func(t *testing.T) {
		var gotKey string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotKey = r.Header.Get("Idempotency-Key")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(chronos.SubmitResponse{
				RunID:        "run-idem-2",
				WorkflowName: "replay",
				TaskCount:    3,
			})
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL)
		resp, err := client.SubmitWorkflowIdempotent(ctx, "run-idem-2", "name: replay", "key-replay")
		if err != nil {
			t.Fatalf("SubmitWorkflowIdempotent() error = %v", err)
		}
		if gotKey != "key-replay" {
			t.Fatalf("expected Idempotency-Key %q, got %q", "key-replay", gotKey)
		}
		if resp.RunID != "run-idem-2" || resp.WorkflowName != "replay" || resp.TaskCount != 3 {
			t.Fatalf("unexpected SubmitResponse: %+v", resp)
		}
	})

	t.Run("empty idempotency key omits header", func(t *testing.T) {
		var hasKey bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, hasKey = r.Header["Idempotency-Key"]
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(chronos.SubmitResponse{RunID: "run-no-key"})
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL)
		if _, err := client.SubmitWorkflowIdempotent(ctx, "run-no-key", "name: demo", ""); err != nil {
			t.Fatalf("SubmitWorkflowIdempotent() error = %v", err)
		}
		if hasKey {
			t.Fatalf("expected Idempotency-Key header to be omitted when key is empty")
		}
	})

	t.Run("409 Conflict returns APIError", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"idempotency key conflict"}`))
		}))
		defer srv.Close()

		client := chronos.NewClient(srv.URL)
		_, err := client.SubmitWorkflowIdempotent(ctx, "run-conflict", "name: demo", "key-conflict")
		var apiErr *chronos.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected *chronos.APIError, got %T (%v)", err, err)
		}
		if apiErr.StatusCode != http.StatusConflict {
			t.Fatalf("expected status 409, got %d", apiErr.StatusCode)
		}
		if apiErr.Message != "idempotency key conflict" {
			t.Fatalf("expected error message %q, got %q", "idempotency key conflict", apiErr.Message)
		}
	})
}
