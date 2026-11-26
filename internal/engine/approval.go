package engine

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	TaskStatusWaitingApproval  TaskStatus = "WAITING_APPROVAL"
	EventTaskApprovalRequested EventType  = "TASK_APPROVAL_REQUESTED"
	EventTaskApproved          EventType  = "TASK_APPROVED"
	EventTaskRejected          EventType  = "TASK_REJECTED"
)

// ApprovalPayload records the actor, decision, and optional comment for a task approval event.
type ApprovalPayload struct {
	TaskID   string `json:"task_id"`
	Actor    string `json:"actor,omitempty"`
	Comment  string `json:"comment,omitempty"`
	Approved bool   `json:"approved"`
}

// RequestTaskApproval transitions a pending task into WAITING_APPROVAL and returns the emitted event.
func RequestTaskApproval(state *RunState, taskID, actor, comment string, ts time.Time) (Event, error) {
	taskState, err := lookupTaskForApproval(state, taskID)
	if err != nil {
		return Event{}, err
	}
	if taskState.Status != TaskStatusPending {
		return Event{}, fmt.Errorf("%w: task %q cannot request approval in status %s", ErrInvalidTransition, taskID, taskState.Status)
	}

	taskState.Status = TaskStatusWaitingApproval
	state.LastSeq++

	return Event{
		RunID:       state.RunID,
		Seq:         state.LastSeq,
		TimestampNs: ts.UnixNano(),
		Type:        EventTaskApprovalRequested,
		TaskID:      taskID,
		Payload: mustMarshal(ApprovalPayload{
			TaskID:  taskID,
			Actor:   actor,
			Comment: comment,
		}),
	}, nil
}

// ResolveTaskApproval approves or rejects a task in WAITING_APPROVAL and returns the emitted event.
func ResolveTaskApproval(state *RunState, taskID, actor, comment string, approved bool, ts time.Time) (Event, error) {
	taskState, err := lookupTaskForApproval(state, taskID)
	if err != nil {
		return Event{}, err
	}
	if taskState.Status != TaskStatusWaitingApproval {
		return Event{}, fmt.Errorf("%w: task %q cannot resolve approval in status %s", ErrInvalidTransition, taskID, taskState.Status)
	}

	evType := EventTaskRejected
	if approved {
		taskState.Status = TaskStatusPending
		evType = EventTaskApproved
	} else {
		taskState.Status = TaskStatusFailed
		taskState.LastError = comment
	}
	state.LastSeq++

	return Event{
		RunID:       state.RunID,
		Seq:         state.LastSeq,
		TimestampNs: ts.UnixNano(),
		Type:        evType,
		TaskID:      taskID,
		Payload: mustMarshal(ApprovalPayload{
			TaskID:   taskID,
			Actor:    actor,
			Comment:  comment,
			Approved: approved,
		}),
	}, nil
}

// ApplyApprovalEvent deterministically applies an approval event onto RunState during replay.
func ApplyApprovalEvent(state *RunState, ev Event) error {
	taskState, err := lookupTaskForApproval(state, ev.TaskID)
	if err != nil {
		return err
	}

	switch ev.Type {
	case EventTaskApprovalRequested:
		if taskState.Status != TaskStatusPending {
			return fmt.Errorf("%w: task %q cannot apply %s in status %s", ErrInvalidTransition, ev.TaskID, ev.Type, taskState.Status)
		}
		taskState.Status = TaskStatusWaitingApproval
	case EventTaskApproved:
		if taskState.Status != TaskStatusWaitingApproval {
			return fmt.Errorf("%w: task %q cannot apply %s in status %s", ErrInvalidTransition, ev.TaskID, ev.Type, taskState.Status)
		}
		taskState.Status = TaskStatusPending
	case EventTaskRejected:
		if taskState.Status != TaskStatusWaitingApproval {
			return fmt.Errorf("%w: task %q cannot apply %s in status %s", ErrInvalidTransition, ev.TaskID, ev.Type, taskState.Status)
		}
		var payload ApprovalPayload
		if len(ev.Payload) > 0 {
			if err := json.Unmarshal(ev.Payload, &payload); err != nil {
				return fmt.Errorf("unmarshal approval payload: %w", err)
			}
		}
		taskState.Status = TaskStatusFailed
		taskState.LastError = payload.Comment
	default:
		return fmt.Errorf("%w: unsupported approval event type %q", ErrInvalidTransition, ev.Type)
	}

	if ev.Seq > state.LastSeq {
		state.LastSeq = ev.Seq
	}
	return nil
}

func lookupTaskForApproval(state *RunState, taskID string) (*TaskState, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: run state is nil", ErrInvalidTransition)
	}
	taskState, ok := state.Tasks[taskID]
	if !ok || taskState == nil {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	return taskState, nil
}
