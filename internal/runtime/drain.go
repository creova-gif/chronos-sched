package runtime

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

// ErrWorkerNotFound is returned when a drain request targets an unregistered worker.
var ErrWorkerNotFound = errors.New("worker not found")

// DrainWorker marks the given worker as draining so it stops receiving new leases,
// and reports the number of in-flight running tasks currently assigned to it.
func (c *Coordinator) DrainWorker(workerID string) (WorkerDrainResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	w, ok := c.workers[workerID]
	if !ok || w == nil {
		return WorkerDrainResponse{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, workerID)
	}
	if !w.Draining {
		w.Draining = true
		c.metrics.IncWorkersDrained()
	}

	inFlight := 0
	for _, ar := range c.runs {
		if ar == nil || ar.state == nil {
			continue
		}
		for _, taskState := range ar.state.Tasks {
			if taskState.Status == engine.TaskStatusRunning && taskState.WorkerID == workerID {
				inFlight++
			}
		}
	}

	return WorkerDrainResponse{
		WorkerID:      workerID,
		Draining:      true,
		InFlightTasks: inFlight,
	}, nil
}

// IsWorkerDraining reports whether the given worker is registered and marked draining.
func (c *Coordinator) IsWorkerDraining(workerID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	w := c.workers[workerID]
	return w != nil && w.Draining
}

// handleWorkerDrain handles POST /v1/workers/drain requests to mark a worker as draining.
func (c *Coordinator) handleWorkerDrain(w http.ResponseWriter, r *http.Request) {
	var req WorkerDrainRequest
	if err := decodeJSONBody(w, r, &req, 0); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	req.WorkerID = strings.TrimSpace(req.WorkerID)
	if req.WorkerID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "worker_id is required"})
		return
	}

	resp, err := c.DrainWorker(req.WorkerID)
	if err != nil {
		if errors.Is(err, ErrWorkerNotFound) {
			writeJSON(w, http.StatusNotFound, ErrorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
