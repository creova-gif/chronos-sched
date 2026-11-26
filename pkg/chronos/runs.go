package chronos

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// RunRecord describes durable lineage and workflow metadata for a workflow run.
type RunRecord struct {
	RunID        string    `json:"run_id"`
	ParentRunID  string    `json:"parent_run_id,omitempty"`
	ForkFromSeq  int64     `json:"fork_from_seq,omitempty"`
	WorkflowName string    `json:"workflow_name"`
	WorkflowHash string    `json:"workflow_hash"`
	WorkflowYAML string    `json:"workflow_yaml"`
	Status       RunStatus `json:"status"`
	CreatedAtNs  int64     `json:"created_at_ns"`
}

// ListRunsOptions configures optional filters for Client.ListRuns.
type ListRunsOptions struct {
	Status string
	Limit  int
}

// ListRuns lists workflow runs from the Coordinator /v1/runs endpoint.
func (c *Client) ListRuns(ctx context.Context, opts ListRunsOptions) ([]RunRecord, error) {
	q := url.Values{}
	if opts.Status != "" {
		q.Set("status", opts.Status)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}

	path := "/v1/runs"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var runs []RunRecord
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &runs); err != nil {
		return nil, err
	}
	return runs, nil
}

func isTerminalRunStatus(status RunStatus) bool {
	switch status {
	case RunStatusCompleted, RunStatusFailed, RunStatusCancelled:
		return true
	default:
		return false
	}
}

// WaitForRun polls GetRun until the run reaches a terminal status (COMPLETED, FAILED, or CANCELLED)
// or the context is cancelled. If pollInterval <= 0, it defaults to 250ms.
func (c *Client) WaitForRun(ctx context.Context, runID string, pollInterval time.Duration) (RunSnapshot, error) {
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}

	snap, err := c.GetRun(ctx, runID)
	if err != nil {
		return RunSnapshot{}, err
	}
	if snap.State != nil && isTerminalRunStatus(snap.State.Status) {
		return snap, nil
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return RunSnapshot{}, ctx.Err()
		case <-ticker.C:
			snap, err := c.GetRun(ctx, runID)
			if err != nil {
				return RunSnapshot{}, err
			}
			if snap.State != nil && isTerminalRunStatus(snap.State.Status) {
				return snap, nil
			}
		}
	}
}
