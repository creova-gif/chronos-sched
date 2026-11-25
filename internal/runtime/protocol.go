package runtime

import "github.com/creova-gif/chronos-sched/internal/engine"

type ActiveLease struct {
	RunID        string `json:"run_id"`
	TaskID       string `json:"task_id"`
	FencingToken int64  `json:"fencing_token"`
}

type WorkerInfo struct {
	WorkerID      string         `json:"worker_id"`
	PID           int            `json:"pid"`
	Concurrency   int            `json:"concurrency"`
	Resources     map[string]int `json:"resources,omitempty"`
	LastHeartbeat int64          `json:"last_heartbeat_ns"`
	Alive         bool           `json:"alive"`
}

type SubmitWorkflowRequest struct {
	RunID        string `json:"run_id,omitempty"`
	WorkflowYAML string `json:"workflow_yaml"`
}

type SubmitWorkflowResponse struct {
	RunID        string `json:"run_id"`
	WorkflowName string `json:"workflow_name"`
	TaskCount    int    `json:"task_count"`
}

type ForkRunRequest struct {
	NewRunID   string                    `json:"new_run_id,omitempty"`
	Checkpoint string                    `json:"checkpoint"`
	Overrides  map[string]map[string]any `json:"overrides,omitempty"`
}

type ForkRunResponse struct {
	RunID         string `json:"run_id"`
	ParentRunID   string `json:"parent_run_id"`
	ForkFromSeq   int64  `json:"fork_from_seq"`
	CheckpointNum int    `json:"checkpoint_num"`
}

type MergeRunRequest struct {
	BranchRunID  string   `json:"branch_run_id"`
	NewRunID     string   `json:"new_run_id,omitempty"`
	Tasks        []string `json:"tasks,omitempty"`
	PreferBranch bool     `json:"prefer_branch,omitempty"`
}

type MergeRunResponse struct {
	RunID                 string           `json:"run_id"`
	BaseRunID             string           `json:"base_run_id"`
	BranchRunID           string           `json:"branch_run_id"`
	MergeFromSeq          int64            `json:"merge_from_seq"`
	Status                engine.RunStatus `json:"status"`
	AdoptedCount          int              `json:"adopted_count"`
	InvalidatedDownstream []string         `json:"invalidated_downstream,omitempty"`
	PlanText              string           `json:"plan_text"`
}

type WorkerRegisterRequest struct {
	WorkerID     string         `json:"worker_id"`
	PID          int            `json:"pid"`
	Concurrency  int            `json:"concurrency"`
	Resources    map[string]int `json:"resources,omitempty"`
	ActiveLeases []ActiveLease  `json:"active_leases,omitempty"`
}

type WorkerRegisterResponse struct {
	OK bool `json:"ok"`
}

type LeasePollRequest struct {
	WorkerID  string         `json:"worker_id"`
	Resources map[string]int `json:"resources,omitempty"`
}

type LeasedTask struct {
	RunID        string         `json:"run_id"`
	TaskID       string         `json:"task_id"`
	Handler      string         `json:"handler"`
	Attempt      int            `json:"attempt"`
	FencingToken int64          `json:"fencing_token"`
	Input        map[string]any `json:"input,omitempty"`
	TimeoutMs    int64          `json:"timeout_ms,omitempty"`
}

type LeasePollResponse struct {
	Lease *LeasedTask `json:"lease,omitempty"`
}

type HeartbeatRequest struct {
	WorkerID     string         `json:"worker_id"`
	PID          int            `json:"pid"`
	Concurrency  int            `json:"concurrency"`
	Resources    map[string]int `json:"resources,omitempty"`
	ActiveLeases []ActiveLease  `json:"active_leases,omitempty"`
}

type HeartbeatResponse struct {
	Renewed int           `json:"renewed"`
	Revoked []ActiveLease `json:"revoked,omitempty"`
}

type TaskStartedRequest struct {
	RunID        string `json:"run_id"`
	TaskID       string `json:"task_id"`
	WorkerID     string `json:"worker_id"`
	FencingToken int64  `json:"fencing_token"`
}

type TaskCompleteRequest struct {
	RunID        string         `json:"run_id"`
	TaskID       string         `json:"task_id"`
	WorkerID     string         `json:"worker_id"`
	FencingToken int64          `json:"fencing_token"`
	Output       map[string]any `json:"output,omitempty"`
}

type TaskFailRequest struct {
	RunID        string `json:"run_id"`
	TaskID       string `json:"task_id"`
	WorkerID     string `json:"worker_id"`
	FencingToken int64  `json:"fencing_token"`
	Error        string `json:"error"`
}

type RunSnapshotResponse struct {
	State   *engine.RunState `json:"state"`
	Workers []WorkerInfo     `json:"workers"`
}

type SSEMessage struct {
	Event     engine.Event     `json:"event"`
	RunStatus engine.RunStatus `json:"run_status"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type RunLifecycleRequest struct {
	Reason string `json:"reason,omitempty"`
}

type RunLifecycleResponse struct {
	RunID  string           `json:"run_id"`
	Status engine.RunStatus `json:"status"`
}
