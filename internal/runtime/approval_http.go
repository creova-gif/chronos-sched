package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

// ApprovalRequest is the JSON payload for requesting or resolving a task approval gate.
type ApprovalRequest struct {
	Actor    string `json:"actor,omitempty"`
	Comment  string `json:"comment,omitempty"`
	Approved bool   `json:"approved"`
}

// ApprovalResponse is the JSON payload returned after a task approval transition.
type ApprovalResponse struct {
	RunID      string            `json:"run_id"`
	TaskID     string            `json:"task_id"`
	TaskStatus engine.TaskStatus `json:"task_status"`
	EventSeq   int64             `json:"event_seq"`
}

func (c *Coordinator) handleRequestTaskApproval(w http.ResponseWriter, r *http.Request) {
	c.handleTaskApproval(w, r, func(state *engine.RunState, taskID string, req ApprovalRequest) (engine.Event, error) {
		return engine.RequestTaskApproval(state, taskID, req.Actor, req.Comment, c.nowFn())
	})
}

func (c *Coordinator) handleResolveTaskApproval(w http.ResponseWriter, r *http.Request) {
	c.handleTaskApproval(w, r, func(state *engine.RunState, taskID string, req ApprovalRequest) (engine.Event, error) {
		return engine.ResolveTaskApproval(state, taskID, req.Actor, req.Comment, req.Approved, c.nowFn())
	})
}

func (c *Coordinator) handleTaskApproval(
	w http.ResponseWriter,
	r *http.Request,
	transition func(*engine.RunState, string, ApprovalRequest) (engine.Event, error),
) {
	id := r.PathValue("id")
	taskID := r.PathValue("task_id")

	var req ApprovalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	c.mu.Lock()
	ar, ok := c.runs[id]
	if !ok {
		c.mu.Unlock()
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("run %q not found", id)})
		return
	}

	prevSeq := ar.state.LastSeq
	var prevStatus engine.TaskStatus
	var prevLastError string
	if ts := ar.state.Tasks[taskID]; ts != nil {
		prevStatus = ts.Status
		prevLastError = ts.LastError
	}

	ev, err := transition(ar.state, taskID, req)
	if err != nil {
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: err.Error()})
		return
	}

	if err := c.store.Append(r.Context(), id, prevSeq, []engine.Event{ev}, ar.state.Status); err != nil {
		// Roll back in-memory state if persistence failed so memory stays consistent with SQLite.
		ar.state.LastSeq = prevSeq
		if ts := ar.state.Tasks[taskID]; ts != nil {
			ts.Status = prevStatus
			ts.LastError = prevLastError
		}
		c.mu.Unlock()
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	ar.state.LastSeq = ev.Seq
	runStatus := ar.state.Status
	taskStatus := ar.state.Tasks[taskID].Status
	c.mu.Unlock()

	c.broadcast(id, []engine.Event{ev}, runStatus)
	writeJSON(w, http.StatusOK, ApprovalResponse{
		RunID:      id,
		TaskID:     taskID,
		TaskStatus: taskStatus,
		EventSeq:   ev.Seq,
	})
}
