package chronos

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// ApprovalResponse is returned by Coordinator task approval endpoints.
type ApprovalResponse struct {
	RunID      string `json:"run_id"`
	TaskID     string `json:"task_id"`
	TaskStatus string `json:"task_status"`
	EventSeq   int64  `json:"event_seq"`
}

type requestApprovalPayload struct {
	Actor   string `json:"actor,omitempty"`
	Comment string `json:"comment,omitempty"`
}

type resolveApprovalPayload struct {
	Actor    string `json:"actor,omitempty"`
	Comment  string `json:"comment,omitempty"`
	Approved bool   `json:"approved"`
}

// RequestTaskApproval transitions a task into WAITING_APPROVAL via the Coordinator API.
func (c *Client) RequestTaskApproval(ctx context.Context, runID, taskID, actor, comment string) (ApprovalResponse, error) {
	path := fmt.Sprintf("/v1/runs/%s/tasks/%s/request-approval", url.PathEscape(runID), url.PathEscape(taskID))
	req := requestApprovalPayload{
		Actor:   actor,
		Comment: comment,
	}
	var resp ApprovalResponse
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return ApprovalResponse{}, err
	}
	return resp, nil
}

// ResolveTaskApproval approves or rejects a task waiting on human approval.
func (c *Client) ResolveTaskApproval(ctx context.Context, runID, taskID, actor, comment string, approved bool) (ApprovalResponse, error) {
	path := fmt.Sprintf("/v1/runs/%s/tasks/%s/approve", url.PathEscape(runID), url.PathEscape(taskID))
	req := resolveApprovalPayload{
		Actor:    actor,
		Comment:  comment,
		Approved: approved,
	}
	var resp ApprovalResponse
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return ApprovalResponse{}, err
	}
	return resp, nil
}
