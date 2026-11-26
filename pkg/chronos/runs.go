package chronos

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
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
