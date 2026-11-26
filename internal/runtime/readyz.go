package runtime

import (
	"net/http"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

// ReadyzResponse is the JSON payload returned by the /readyz readiness probe.
type ReadyzResponse struct {
	Status     string `json:"status"`
	ActiveRuns int    `json:"active_runs"`
	Workers    int    `json:"workers"`
	Error      string `json:"error,omitempty"`
}

// handleReadyz verifies store connectivity (preferring store.HealthChecker
// for an O(1) Ping and falling back to ListRuns) and reports active run and
// worker counts. It returns 200 OK when ready and 503 Service Unavailable when
// the backing store returns an error.
func (c *Coordinator) handleReadyz(w http.ResponseWriter, r *http.Request) {
	var err error
	if hc, ok := c.store.(store.HealthChecker); ok {
		err = hc.Ping(r.Context())
	} else {
		_, err = c.store.ListRuns(r.Context(), engine.RunStatusRunning)
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, ReadyzResponse{
			Status: "not_ready",
			Error:  err.Error(),
		})
		return
	}

	c.mu.Lock()
	activeRuns := len(c.runs)
	workers := len(c.workers)
	c.mu.Unlock()

	writeJSON(w, http.StatusOK, ReadyzResponse{
		Status:     "ready",
		ActiveRuns: activeRuns,
		Workers:    workers,
	})
}
