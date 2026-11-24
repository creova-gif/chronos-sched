package engine

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	DefaultHeartbeatInterval = 1 * time.Second
	DefaultLeaseTTL          = 3 * time.Second
	DefaultReaperTick        = 250 * time.Millisecond
)

var (
	ErrStaleFencingToken     = errors.New("stale fencing token")
	ErrOverrideCompletedTask = errors.New("cannot override task already completed at or before fork checkpoint")
	ErrCheckpointNotFound    = errors.New("checkpoint not found")
	ErrTaskNotFound          = errors.New("task not found")
	ErrRunTerminal           = errors.New("run is already in a terminal state")
	ErrInvalidTransition     = errors.New("invalid state transition")
)

type EventType string

const (
	EventRunStarted           EventType = "RUN_STARTED"
	EventTaskLeased           EventType = "TASK_LEASED"
	EventTaskStarted          EventType = "TASK_STARTED"
	EventTaskCompleted        EventType = "TASK_COMPLETED"
	EventTaskFailed           EventType = "TASK_FAILED"
	EventCheckpointSaved      EventType = "CHECKPOINT_SAVED"
	EventLeaseExpired         EventType = "LEASE_EXPIRED"
	EventTaskOutcomeAmbiguous EventType = "TASK_OUTCOME_AMBIGUOUS"
	EventStaleFencingToken    EventType = "STALE_FENCING_TOKEN"
	EventTimelineForked       EventType = "TIMELINE_FORKED"
	EventRunCompleted         EventType = "RUN_COMPLETED"
	EventRunFailed            EventType = "RUN_FAILED"
)

type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusCompleted RunStatus = "COMPLETED"
	RunStatusFailed    RunStatus = "FAILED"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusLeased    TaskStatus = "LEASED"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
)

type TaskSpec struct {
	ID             string         `yaml:"id" json:"id"`
	Handler        string         `yaml:"handler" json:"handler"`
	DependsOn      []string       `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Priority       int            `yaml:"priority,omitempty" json:"priority,omitempty"`
	MaxRetries     int            `yaml:"max_retries,omitempty" json:"max_retries,omitempty"`
	TimeoutMs      int64          `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	CheckpointName string         `yaml:"checkpoint_name,omitempty" json:"checkpoint_name,omitempty"`
	Params         map[string]any `yaml:"params,omitempty" json:"params,omitempty"`
}

type WorkflowSpec struct {
	Name  string     `yaml:"name" json:"name"`
	Tasks []TaskSpec `yaml:"tasks" json:"tasks"`
}

type DAG struct {
	Spec         WorkflowSpec
	TasksByID    map[string]TaskSpec
	Dependents   map[string][]string
	TopoOrder    []string
	WorkflowHash string
}

type Event struct {
	RunID        string          `json:"run_id"`
	Seq          int64           `json:"seq"`
	TimestampNs  int64           `json:"ts_ns"`
	Type         EventType       `json:"event_type"`
	TaskID       string          `json:"task_id,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	FencingToken int64           `json:"fencing_token,omitempty"`
	WorkerID     string          `json:"worker_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

type RunStartedPayload struct {
	WorkflowName string `json:"workflow_name"`
	WorkflowHash string `json:"workflow_hash"`
}

type TaskLeasedPayload struct {
	LeaseExpiresAtNs int64 `json:"lease_expires_at_ns"`
}

type TaskCompletedPayload struct {
	Output map[string]any `json:"output,omitempty"`
}

type TaskFailedPayload struct {
	Error     string `json:"error"`
	WillRetry bool   `json:"will_retry"`
}

type CheckpointSavedPayload struct {
	CheckpointNum  int    `json:"checkpoint_num"`
	CheckpointName string `json:"checkpoint_name,omitempty"`
	TaskID         string `json:"task_id"`
}

type LeaseExpiredPayload struct {
	Reason    string `json:"reason"`
	WillRetry bool   `json:"will_retry"`
}

type StaleFencingPayload struct {
	ExpectedToken int64  `json:"expected_token"`
	ReceivedToken int64  `json:"received_token"`
	Reason        string `json:"reason"`
}

type TimelineForkedPayload struct {
	ParentRunID    string                    `json:"parent_run_id"`
	ForkFromSeq    int64                     `json:"fork_from_seq"`
	CheckpointNum  int                       `json:"checkpoint_num"`
	CheckpointName string                    `json:"checkpoint_name,omitempty"`
	Overrides      map[string]map[string]any `json:"overrides,omitempty"`
}

type RunFailedPayload struct {
	Reason string `json:"reason"`
}

type CheckpointInfo struct {
	Number int    `json:"number"`
	Name   string `json:"name,omitempty"`
	Seq    int64  `json:"seq"`
	TaskID string `json:"task_id"`
}

type TaskState struct {
	ID               string         `json:"id"`
	Status           TaskStatus     `json:"status"`
	Attempt          int            `json:"attempt"`
	FencingToken     int64          `json:"fencing_token"`
	WorkerID         string         `json:"worker_id,omitempty"`
	LeaseExpiresAtNs int64          `json:"lease_expires_at_ns,omitempty"`
	Output           map[string]any `json:"output,omitempty"`
	LastError        string         `json:"last_error,omitempty"`
	AmbiguousCount   int            `json:"ambiguous_count,omitempty"`
	CompletedSeq     int64          `json:"completed_seq,omitempty"`
}

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

// Command represents a deterministic input to Step.
type Command interface {
	isCommand()
}

type CmdStartRun struct {
	RunID string
}

type CmdLeaseNextTask struct {
	WorkerID string
	LeaseTTL time.Duration
}

type CmdMarkTaskStarted struct {
	TaskID       string
	WorkerID     string
	FencingToken int64
}

type CmdCompleteTask struct {
	TaskID       string
	WorkerID     string
	FencingToken int64
	Output       map[string]any
}

type CmdFailTask struct {
	TaskID       string
	WorkerID     string
	FencingToken int64
	ErrMsg       string
}

type CmdExpireLeases struct{}

type CmdForkRun struct {
	NewRunID           string
	CheckpointSelector string
	Overrides          map[string]map[string]any
}

func (CmdStartRun) isCommand()        {}
func (CmdLeaseNextTask) isCommand()   {}
func (CmdMarkTaskStarted) isCommand() {}
func (CmdCompleteTask) isCommand()    {}
func (CmdFailTask) isCommand()        {}
func (CmdExpireLeases) isCommand()    {}
func (CmdForkRun) isCommand()         {}

type EffectKind string

const (
	EffectDispatchTask EffectKind = "DISPATCH_TASK"
	EffectRunTerminal  EffectKind = "RUN_TERMINAL"
)

type Effect struct {
	Kind         EffectKind
	RunID        string
	TaskID       string
	WorkerID     string
	Attempt      int
	FencingToken int64
	Status       RunStatus
}
