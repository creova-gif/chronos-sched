package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/runtime"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestE2E_DLQAutoRecordRequeueAndObservability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "dlq_e2e.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const authToken = "e2e-tok"
	coord := runtime.NewCoordinator(st, runtime.CoordinatorConfig{
		LeaseTTL:   2 * time.Second,
		ReaperTick: 50 * time.Millisecond,
		AuthToken:  authToken,
	})
	go coord.StartReaper(ctx)

	srv := httptest.NewServer(coord.Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// 1. Verify GET /readyz returns 200 OK with status=="ready" and echoes Traceparent / X-Request-Id.
	const (
		traceID       = "0123456789abcdef0123456789abcdef"
		incomingTrace = "00-" + traceID + "-0123456789abcdef-01"
		incomingReqID = "dlq-e2e-req-1"
	)
	readyReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("new /readyz request: %v", err)
	}
	readyReq.Header.Set("Authorization", "Bearer "+authToken)
	readyReq.Header.Set("Traceparent", incomingTrace)
	readyReq.Header.Set("X-Request-Id", incomingReqID)

	readyResp, err := client.Do(readyReq)
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	readyBody, _ := io.ReadAll(readyResp.Body)
	_ = readyResp.Body.Close()

	if readyResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz status = %d, want 200: %s", readyResp.StatusCode, string(readyBody))
	}
	var readyPayload runtime.ReadyzResponse
	if err := json.Unmarshal(readyBody, &readyPayload); err != nil {
		t.Fatalf("unmarshal /readyz: %v", err)
	}
	if readyPayload.Status != "ready" {
		t.Fatalf("readyz status = %q, want %q", readyPayload.Status, "ready")
	}
	if gotTP := readyResp.Header.Get("Traceparent"); !strings.HasPrefix(gotTP, "00-"+traceID+"-") {
		t.Fatalf("Traceparent = %q, want prefix %q", gotTP, "00-"+traceID+"-")
	}
	if gotReqID := readyResp.Header.Get("X-Request-Id"); gotReqID != incomingReqID {
		t.Fatalf("X-Request-Id = %q, want %q", gotReqID, incomingReqID)
	}

	// 2. Verify /v1/dlq requires Bearer auth (401 without token).
	unauthResp, err := doAuthJSON(ctx, client, http.MethodGet, srv.URL+"/v1/dlq", "", nil)
	if err != nil {
		t.Fatalf("unauthenticated GET /v1/dlq: %v", err)
	}
	_ = unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /v1/dlq status = %d, want 401", unauthResp.StatusCode)
	}

	// 3. Submit a 1-task workflow (max_retries: 0, handler: "fail_always").
	wfYAML := `name: dlq-e2e-wf
tasks:
  - id: task1
    handler: fail_always
    max_retries: 0
`
	subBody, err := json.Marshal(runtime.SubmitWorkflowRequest{WorkflowYAML: wfYAML})
	if err != nil {
		t.Fatalf("marshal submit request: %v", err)
	}
	subHTTPResp, err := doAuthJSON(ctx, client, http.MethodPost, srv.URL+"/v1/workflows/run", authToken, subBody)
	if err != nil {
		t.Fatalf("POST /v1/workflows/run: %v", err)
	}
	subRespBytes, _ := io.ReadAll(subHTTPResp.Body)
	_ = subHTTPResp.Body.Close()
	if subHTTPResp.StatusCode != http.StatusCreated && subHTTPResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/workflows/run status = %d, body = %s", subHTTPResp.StatusCode, string(subRespBytes))
	}
	var subResp runtime.SubmitWorkflowResponse
	if err := json.Unmarshal(subRespBytes, &subResp); err != nil {
		t.Fatalf("unmarshal submit response: %v", err)
	}

	// 4. Start Worker with AuthToken and "fail_always" handler failing on call 1, succeeding on call 2.
	var invocations atomic.Int32
	worker := runtime.NewWorker(runtime.WorkerConfig{
		WorkerID:          "worker-dlq-e2e",
		CoordinatorURL:    srv.URL,
		AuthToken:         authToken,
		Concurrency:       2,
		HeartbeatInterval: 200 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	})
	worker.RegisterHandler("fail_always", func(_ context.Context, _ map[string]any) (map[string]any, error) {
		if invocations.Add(1) == 1 {
			return nil, fmt.Errorf("simulated failure on first attempt")
		}
		return map[string]any{"recovered": true}, nil
	})
	go func() {
		_ = worker.Run(ctx)
	}()

	// 5. Wait for the first run to reach FAILED.
	waitForRunStatus(t, ctx, client, srv.URL, authToken, subResp.RunID, engine.RunStatusFailed)

	// Verify GET /v1/dlq returns 1 active DeadLetterEntry.
	dlqResp := getDLQ(t, ctx, client, srv.URL+"/v1/dlq", authToken)
	if len(dlqResp.Entries) != 1 {
		t.Fatalf("len(active DLQ entries) = %d, want 1 (%+v)", len(dlqResp.Entries), dlqResp.Entries)
	}
	entry := dlqResp.Entries[0]
	if entry.RunID != subResp.RunID || entry.TaskID != "task1" || entry.Attempt != 1 || entry.Requeued {
		t.Fatalf("unexpected DLQ entry: %+v (want run_id=%q, task_id=%q, attempt=1, requeued=false)",
			entry, subResp.RunID, "task1")
	}

	// 6. Call POST /v1/dlq/{id}/requeue -> verify 201 Created and a new requeued_run_id.
	requeueURL := fmt.Sprintf("%s/v1/dlq/%d/requeue", srv.URL, entry.ID)
	reqHTTPResp, err := doAuthJSON(ctx, client, http.MethodPost, requeueURL, authToken, nil)
	if err != nil {
		t.Fatalf("POST %s: %v", requeueURL, err)
	}
	reqBytes, _ := io.ReadAll(reqHTTPResp.Body)
	_ = reqHTTPResp.Body.Close()
	if reqHTTPResp.StatusCode != http.StatusCreated {
		t.Fatalf("POST %s status = %d, want 201: %s", requeueURL, reqHTTPResp.StatusCode, string(reqBytes))
	}
	var reqResp runtime.RequeueDeadLetterResponse
	if err := json.Unmarshal(reqBytes, &reqResp); err != nil {
		t.Fatalf("unmarshal requeue response: %v", err)
	}
	if reqResp.DeadLetterID != entry.ID {
		t.Fatalf("dead_letter_id = %d, want %d", reqResp.DeadLetterID, entry.ID)
	}
	if reqResp.RequeuedRunID == "" || reqResp.RequeuedRunID == subResp.RunID {
		t.Fatalf("unexpected requeued_run_id = %q", reqResp.RequeuedRunID)
	}

	// 7. Wait for the requeued run to reach COMPLETED.
	waitForRunStatus(t, ctx, client, srv.URL, authToken, reqResp.RequeuedRunID, engine.RunStatusCompleted)

	// 8. Verify GET /v1/dlq returns 0 active entries, while GET /v1/dlq?all=true returns the requeued entry.
	activeAfter := getDLQ(t, ctx, client, srv.URL+"/v1/dlq", authToken)
	if activeAfter.Entries == nil || len(activeAfter.Entries) != 0 {
		t.Fatalf("expected empty non-nil active DLQ entries, got %#v", activeAfter.Entries)
	}

	allAfter := getDLQ(t, ctx, client, srv.URL+"/v1/dlq?all=true", authToken)
	if len(allAfter.Entries) != 1 {
		t.Fatalf("len(all DLQ entries) = %d, want 1", len(allAfter.Entries))
	}
	got := allAfter.Entries[0]
	if !got.Requeued || got.RequeuedRunID != reqResp.RequeuedRunID {
		t.Fatalf("expected requeued=true and requeued_run_id=%q, got %+v", reqResp.RequeuedRunID, got)
	}
}

func doAuthJSON(ctx context.Context, client *http.Client, method, url, token string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return client.Do(req)
}

func getDLQ(t *testing.T, ctx context.Context, client *http.Client, url, token string) runtime.ListDeadLettersResponse {
	t.Helper()
	resp, err := doAuthJSON(ctx, client, http.MethodGet, url, token, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200: %s", url, resp.StatusCode, string(body))
	}
	var out runtime.ListDeadLettersResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal %s: %v", url, err)
	}
	return out
}

func waitForRunStatus(t *testing.T, ctx context.Context, client *http.Client, baseURL, token, runID string, want engine.RunStatus) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	var lastStatus engine.RunStatus
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for run %s to reach %s (last status: %s): %v", runID, want, lastStatus, ctx.Err())
		case <-ticker.C:
			resp, err := doAuthJSON(ctx, client, http.MethodGet, baseURL+"/v1/runs/"+runID, token, nil)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				continue
			}
			var snap runtime.RunSnapshotResponse
			if err := json.Unmarshal(body, &snap); err != nil || snap.State == nil {
				continue
			}
			lastStatus = snap.State.Status
			if lastStatus == want {
				return
			}
		}
	}
}
