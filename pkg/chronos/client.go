package chronos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// RunStatus represents the lifecycle state of a workflow run.
type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusPaused    RunStatus = "PAUSED"
	RunStatusCompleted RunStatus = "COMPLETED"
	RunStatusFailed    RunStatus = "FAILED"
	RunStatusCancelled RunStatus = "CANCELLED"
)

// TaskStatus represents the execution state of a task within a run.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusLeased    TaskStatus = "LEASED"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
)

// APIError represents a non-2xx HTTP error response from the Coordinator API.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("chronos api error (status %d): %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("chronos api error (status %d)", e.StatusCode)
}

// SubmitResponse is returned when a workflow run is submitted.
type SubmitResponse struct {
	RunID        string `json:"run_id"`
	WorkflowName string `json:"workflow_name"`
	TaskCount    int    `json:"task_count"`
}

// ForkResponse is returned when a run is forked from a checkpoint.
type ForkResponse struct {
	RunID         string `json:"run_id"`
	ParentRunID   string `json:"parent_run_id"`
	ForkFromSeq   int64  `json:"fork_from_seq"`
	CheckpointNum int    `json:"checkpoint_num"`
}

// MergeResponse is returned when a branch timeline is merged onto a base run.
type MergeResponse struct {
	RunID                 string    `json:"run_id"`
	BaseRunID             string    `json:"base_run_id"`
	BranchRunID           string    `json:"branch_run_id"`
	MergeFromSeq          int64     `json:"merge_from_seq"`
	Status                RunStatus `json:"status"`
	AdoptedCount          int       `json:"adopted_count"`
	InvalidatedDownstream []string  `json:"invalidated_downstream,omitempty"`
	PlanText              string    `json:"plan_text"`
}

// LifecycleResponse is returned by pause, resume, and cancel operations.
type LifecycleResponse struct {
	RunID  string    `json:"run_id"`
	Status RunStatus `json:"status"`
}

// WorkerInfo describes a registered worker and its liveness state.
type WorkerInfo struct {
	WorkerID      string         `json:"worker_id"`
	PID           int            `json:"pid"`
	Concurrency   int            `json:"concurrency"`
	Resources     map[string]int `json:"resources,omitempty"`
	LastHeartbeat int64          `json:"last_heartbeat_ns"`
	Alive         bool           `json:"alive"`
}

// CheckpointInfo describes a saved checkpoint within a run.
type CheckpointInfo struct {
	Number int    `json:"number"`
	Name   string `json:"name,omitempty"`
	Seq    int64  `json:"seq"`
	TaskID string `json:"task_id"`
}

// TaskState describes the state of a single task in a run snapshot.
type TaskState struct {
	ID               string         `json:"id"`
	Status           TaskStatus     `json:"status"`
	Attempt          int            `json:"attempt"`
	FencingToken     int64          `json:"fencing_token"`
	WorkerID         string         `json:"worker_id,omitempty"`
	LeaseExpiresAtNs int64          `json:"lease_expires_at_ns,omitempty"`
	RetryAfterNs     int64          `json:"retry_after_ns,omitempty"`
	StartedAtNs      int64          `json:"started_at_ns,omitempty"`
	Output           map[string]any `json:"output,omitempty"`
	LastError        string         `json:"last_error,omitempty"`
	AmbiguousCount   int            `json:"ambiguous_count,omitempty"`
	CompletedSeq     int64          `json:"completed_seq,omitempty"`
}

// RunState describes the folded state of a workflow run.
type RunState struct {
	RunID             string                    `json:"run_id"`
	ParentRunID       string                    `json:"parent_run_id,omitempty"`
	ForkFromSeq       int64                     `json:"fork_from_seq,omitempty"`
	WorkflowName      string                    `json:"workflow_name"`
	WorkflowHash      string                    `json:"workflow_hash"`
	Status            RunStatus                 `json:"status"`
	LastSeq           int64                     `json:"last_seq"`
	NextFencingToken  int64                     `json:"next_fencing_token"`
	NextCheckpointNum int                       `json:"next_checkpoint_num"`
	Tasks             map[string]*TaskState     `json:"tasks"`
	TopoOrder         []string                  `json:"topo_order"`
	Checkpoints       []CheckpointInfo          `json:"checkpoints"`
	Overrides         map[string]map[string]any `json:"overrides,omitempty"`
	FailureReason     string                    `json:"failure_reason,omitempty"`
}

// RunSnapshot is returned by GetRun and contains the run state and known workers.
type RunSnapshot struct {
	State   *RunState    `json:"state"`
	Workers []WorkerInfo `json:"workers"`
}

type submitWorkflowRequest struct {
	RunID        string `json:"run_id,omitempty"`
	WorkflowYAML string `json:"workflow_yaml"`
}

type forkRunRequest struct {
	NewRunID   string                    `json:"new_run_id,omitempty"`
	Checkpoint string                    `json:"checkpoint"`
	Overrides  map[string]map[string]any `json:"overrides,omitempty"`
}

type mergeRunRequest struct {
	BranchRunID  string   `json:"branch_run_id"`
	NewRunID     string   `json:"new_run_id,omitempty"`
	Tasks        []string `json:"tasks,omitempty"`
	PreferBranch bool     `json:"prefer_branch,omitempty"`
}

type runLifecycleRequest struct {
	Reason string `json:"reason,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Client is a typed HTTP client for the ChronosSched Coordinator API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient constructs a Client targeting baseURL with a 10s default HTTP timeout.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Health checks the Coordinator /healthz endpoint.
func (c *Client) Health(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodGet, "/healthz", nil, nil)
}

// SubmitWorkflow submits a workflow YAML definition to start a new run.
func (c *Client) SubmitWorkflow(ctx context.Context, runID, workflowYAML string) (SubmitResponse, error) {
	req := submitWorkflowRequest{
		RunID:        runID,
		WorkflowYAML: workflowYAML,
	}
	var resp SubmitResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/workflows/run", req, &resp); err != nil {
		return SubmitResponse{}, err
	}
	return resp, nil
}

// GetRun retrieves the current snapshot and worker list for runID.
func (c *Client) GetRun(ctx context.Context, runID string) (RunSnapshot, error) {
	var resp RunSnapshot
	path := fmt.Sprintf("/v1/runs/%s", url.PathEscape(runID))
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return RunSnapshot{}, err
	}
	return resp, nil
}

// ForkRun creates a new run branched from parentRunID at the specified checkpoint.
func (c *Client) ForkRun(ctx context.Context, parentRunID, checkpoint string, overrides map[string]map[string]any, newRunID string) (ForkResponse, error) {
	req := forkRunRequest{
		NewRunID:   newRunID,
		Checkpoint: checkpoint,
		Overrides:  overrides,
	}
	var resp ForkResponse
	path := fmt.Sprintf("/v1/runs/%s/fork", url.PathEscape(parentRunID))
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return ForkResponse{}, err
	}
	return resp, nil
}

// MergeRun merges completed tasks from branchRunID onto baseRunID, creating a new run.
func (c *Client) MergeRun(ctx context.Context, baseRunID, branchRunID string, tasks []string, preferBranch bool, newRunID string) (MergeResponse, error) {
	req := mergeRunRequest{
		BranchRunID:  branchRunID,
		NewRunID:     newRunID,
		Tasks:        tasks,
		PreferBranch: preferBranch,
	}
	var resp MergeResponse
	path := fmt.Sprintf("/v1/runs/%s/merge", url.PathEscape(baseRunID))
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return MergeResponse{}, err
	}
	return resp, nil
}

// PauseRun pauses an active run.
func (c *Client) PauseRun(ctx context.Context, runID, reason string) (LifecycleResponse, error) {
	return c.doLifecycle(ctx, runID, "pause", reason)
}

// ResumeRun resumes a paused run.
func (c *Client) ResumeRun(ctx context.Context, runID, reason string) (LifecycleResponse, error) {
	return c.doLifecycle(ctx, runID, "resume", reason)
}

// CancelRun cancels a non-terminal run.
func (c *Client) CancelRun(ctx context.Context, runID, reason string) (LifecycleResponse, error) {
	return c.doLifecycle(ctx, runID, "cancel", reason)
}

func (c *Client) doLifecycle(ctx context.Context, runID, action, reason string) (LifecycleResponse, error) {
	req := runLifecycleRequest{Reason: reason}
	var resp LifecycleResponse
	path := fmt.Sprintf("/v1/runs/%s/%s", url.PathEscape(runID), action)
	if err := c.doJSON(ctx, http.MethodPost, path, req, &resp); err != nil {
		return LifecycleResponse{}, err
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, reqBody, respBody any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp errorResponse
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return &APIError{StatusCode: resp.StatusCode, Message: errResp.Error}
		}
		return &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(data))}
	}

	if respBody != nil && len(data) > 0 {
		if err := json.Unmarshal(data, respBody); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
