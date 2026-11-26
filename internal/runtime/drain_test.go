package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestDrainWorkerAndInFlightCount(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "drain.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := NewCoordinator(st, CoordinatorConfig{})

	t.Run("unknown worker returns ErrWorkerNotFound and 404", func(t *testing.T) {
		if coord.IsWorkerDraining("missing-worker") {
			t.Fatal("IsWorkerDraining(missing-worker) = true, want false")
		}
		if _, err := coord.DrainWorker("missing-worker"); !errors.Is(err, ErrWorkerNotFound) {
			t.Fatalf("DrainWorker(missing-worker) err = %v, want ErrWorkerNotFound", err)
		}

		body, _ := json.Marshal(WorkerDrainRequest{WorkerID: "missing-worker"})
		req := httptest.NewRequest(http.MethodPost, "/v1/workers/drain", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		coord.handleWorkerDrain(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("empty worker_id returns 400", func(t *testing.T) {
		body, _ := json.Marshal(WorkerDrainRequest{WorkerID: ""})
		req := httptest.NewRequest(http.MethodPost, "/v1/workers/drain", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		coord.handleWorkerDrain(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("registered worker with active running task drains with in_flight_tasks=1", func(t *testing.T) {
		const workerID = "worker-drain-1"

		regBody, _ := json.Marshal(WorkerRegisterRequest{
			WorkerID:    workerID,
			PID:         1234,
			Concurrency: 2,
		})
		regReq := httptest.NewRequest(http.MethodPost, "/v1/workers/register", bytes.NewReader(regBody))
		regRec := httptest.NewRecorder()
		coord.handleWorkerRegister(regRec, regReq)
		if regRec.Code != http.StatusOK {
			t.Fatalf("register status = %d, want 200; body=%s", regRec.Code, regRec.Body.String())
		}

		wfYAML := `
name: drain-test
tasks:
  - id: step1
    handler: sim
    params:
      duration_ms: 100
`
		sub, err := coord.SubmitWorkflow(ctx, SubmitWorkflowRequest{
			RunID:        "run_drain_1",
			WorkflowYAML: wfYAML,
		})
		if err != nil {
			t.Fatalf("SubmitWorkflow: %v", err)
		}

		// Lease step1 to worker-drain-1.
		pollBody, _ := json.Marshal(LeasePollRequest{WorkerID: workerID})
		pollReq := httptest.NewRequest(http.MethodPost, "/v1/workers/poll", bytes.NewReader(pollBody))
		pollRec := httptest.NewRecorder()
		coord.handleWorkerPoll(pollRec, pollReq)
		if pollRec.Code != http.StatusOK {
			t.Fatalf("lease poll status = %d, want 200; body=%s", pollRec.Code, pollRec.Body.String())
		}
		var pollResp LeasePollResponse
		if err := json.Unmarshal(pollRec.Body.Bytes(), &pollResp); err != nil {
			t.Fatalf("unmarshal LeasePollResponse: %v", err)
		}
		if pollResp.Lease == nil || pollResp.Lease.TaskID != "step1" {
			t.Fatalf("expected lease for step1, got %+v", pollResp.Lease)
		}

		// Mark task started so status transitions to TaskStatusRunning.
		startBody, _ := json.Marshal(TaskStartedRequest{
			RunID:        sub.RunID,
			TaskID:       "step1",
			WorkerID:     workerID,
			FencingToken: pollResp.Lease.FencingToken,
		})
		startReq := httptest.NewRequest(http.MethodPost, "/v1/tasks/started", bytes.NewReader(startBody))
		startRec := httptest.NewRecorder()
		coord.handleTaskStarted(startRec, startReq)
		if startRec.Code != http.StatusOK {
			t.Fatalf("task started status = %d, want 200; body=%s", startRec.Code, startRec.Body.String())
		}

		// Verify task is indeed TaskStatusRunning before draining.
		coord.mu.Lock()
		ts := coord.runs[sub.RunID].state.Tasks["step1"]
		status := ts.Status
		coord.mu.Unlock()
		if status != engine.TaskStatusRunning {
			t.Fatalf("task status = %s, want %s", status, engine.TaskStatusRunning)
		}

		// Drain via HTTP handler.
		drainBody, _ := json.Marshal(WorkerDrainRequest{WorkerID: workerID})
		drainReq := httptest.NewRequest(http.MethodPost, "/v1/workers/drain", bytes.NewReader(drainBody))
		drainRec := httptest.NewRecorder()
		coord.handleWorkerDrain(drainRec, drainReq)
		if drainRec.Code != http.StatusOK {
			t.Fatalf("drain status = %d, want 200; body=%s", drainRec.Code, drainRec.Body.String())
		}

		var drainResp WorkerDrainResponse
		if err := json.Unmarshal(drainRec.Body.Bytes(), &drainResp); err != nil {
			t.Fatalf("unmarshal WorkerDrainResponse: %v", err)
		}
		if drainResp.WorkerID != workerID || !drainResp.Draining || drainResp.InFlightTasks != 1 {
			t.Fatalf("unexpected WorkerDrainResponse: %+v", drainResp)
		}
		if !coord.IsWorkerDraining(workerID) {
			t.Fatal("IsWorkerDraining(workerID) = false, want true")
		}
	})
}
