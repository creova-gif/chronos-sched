package runtime

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/creova-gif/chronos-sched/internal/store"
)

// ListDeadLettersResponse is the JSON payload returned by GET /v1/dlq.
type ListDeadLettersResponse struct {
	Entries []store.DeadLetterEntry `json:"entries"`
}

// RequeueDeadLetterResponse is the JSON payload returned by POST /v1/dlq/{id}/requeue.
type RequeueDeadLetterResponse struct {
	DeadLetterID  int64  `json:"dead_letter_id"`
	RequeuedRunID string `json:"requeued_run_id"`
}

func (c *Coordinator) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	dlq, ok := c.store.(store.DLQStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "store does not support dead-letter queue"})
		return
	}

	allParam := r.URL.Query().Get("all")
	includeAll := allParam == "true" || allParam == "1"

	entries, err := dlq.ListDeadLetters(r.Context(), includeAll)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	if ts, ok := c.store.(store.TenantStore); ok {
		tenant := TenantFromContext(r.Context())
		filtered := make([]store.DeadLetterEntry, 0, len(entries))
		for _, entry := range entries {
			owner, err := ts.GetRunTenant(r.Context(), entry.RunID)
			if err == nil && owner == tenant {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}
	if entries == nil {
		entries = []store.DeadLetterEntry{}
	}

	writeJSON(w, http.StatusOK, ListDeadLettersResponse{Entries: entries})
}

func (c *Coordinator) handleRequeueDeadLetter(w http.ResponseWriter, r *http.Request) {
	dlq, ok := c.store.(store.DLQStore)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "store does not support dead-letter queue"})
		return
	}

	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid dead-letter id"})
		return
	}

	entries, err := dlq.ListDeadLetters(r.Context(), true)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	var target *store.DeadLetterEntry
	for i := range entries {
		if entries[i].ID == id {
			target = &entries[i]
			break
		}
	}
	if target == nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("dead-letter entry %d not found", id)})
		return
	}
	if !c.authorizeRunTenant(w, r, target.RunID) {
		return
	}
	if target.Requeued {
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: "dead-letter entry already requeued"})
		return
	}

	rec, err := c.store.GetRun(r.Context(), target.RunID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	subResp, err := c.SubmitWorkflow(r.Context(), SubmitWorkflowRequest{
		WorkflowYAML: rec.WorkflowYAML,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	c.assignRunTenant(r.Context(), subResp.RunID)

	if err := dlq.MarkDeadLetterRequeued(r.Context(), target.ID, subResp.RunID); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, RequeueDeadLetterResponse{
		DeadLetterID:  target.ID,
		RequeuedRunID: subResp.RunID,
	})
}
