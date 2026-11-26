package runtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestListWorkersHTTP(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workers.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	coord := NewCoordinator(st, CoordinatorConfig{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/workers", coord.handleListWorkers)

	// Empty state must return {"workers":[]} (non-null JSON array).
	req := httptest.NewRequest(http.MethodGet, "/v1/workers", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("empty GET /v1/workers status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"workers":[]}` {
		t.Fatalf("empty GET /v1/workers body = %q, want %q", got, `{"workers":[]}`)
	}

	// Register w-b then w-a with resources, then drain w-a.
	regWorkers := []WorkerRegisterRequest{
		{
			WorkerID:    "w-b",
			PID:         2002,
			Concurrency: 4,
			Resources:   map[string]int{"cpu": 4, "gpu": 1},
		},
		{
			WorkerID:    "w-a",
			PID:         2001,
			Concurrency: 2,
			Resources:   map[string]int{"cpu": 2, "mem_gb": 8},
		},
	}
	for _, rw := range regWorkers {
		body, err := json.Marshal(rw)
		if err != nil {
			t.Fatalf("marshal WorkerRegisterRequest(%s): %v", rw.WorkerID, err)
		}
		regReq := httptest.NewRequest(http.MethodPost, "/v1/workers/register", strings.NewReader(string(body)))
		regRec := httptest.NewRecorder()
		coord.handleWorkerRegister(regRec, regReq)
		if regRec.Code != http.StatusOK {
			t.Fatalf("register(%s) status = %d, want 200; body=%s", rw.WorkerID, regRec.Code, regRec.Body.String())
		}
	}

	if _, err := coord.DrainWorker("w-a"); err != nil {
		t.Fatalf("DrainWorker(w-a): %v", err)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/workers", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/workers status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var resp ListWorkersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal ListWorkersResponse: %v", err)
	}
	if len(resp.Workers) != 2 {
		t.Fatalf("len(Workers) = %d, want 2", len(resp.Workers))
	}

	// Verify sorted order (w-a, w-b), Alive, Draining, and Resources.
	wA := resp.Workers[0]
	wB := resp.Workers[1]
	if wA.WorkerID != "w-a" || wB.WorkerID != "w-b" {
		t.Fatalf("workers order = [%s, %s], want [w-a, w-b]", wA.WorkerID, wB.WorkerID)
	}
	if !wA.Alive || !wB.Alive {
		t.Fatalf("Alive = (%v, %v), want (true, true)", wA.Alive, wB.Alive)
	}
	if !wA.Draining || wB.Draining {
		t.Fatalf("Draining = (%v, %v), want (true, false)", wA.Draining, wB.Draining)
	}
	if wA.Resources["cpu"] != 2 || wA.Resources["mem_gb"] != 8 {
		t.Fatalf("w-a Resources = %v, want map[cpu:2 mem_gb:8]", wA.Resources)
	}
	if wB.Resources["cpu"] != 4 || wB.Resources["gpu"] != 1 {
		t.Fatalf("w-b Resources = %v, want map[cpu:4 gpu:1]", wB.Resources)
	}

	// Verify ListWorkers clones Resources so caller mutations do not affect coordinator state.
	listed := coord.ListWorkers()
	listed[0].Resources["cpu"] = 999
	listedAgain := coord.ListWorkers()
	if listedAgain[0].Resources["cpu"] != 2 {
		t.Fatalf("Resources not cloned: got cpu=%d after caller mutation, want 2", listedAgain[0].Resources["cpu"])
	}
}
