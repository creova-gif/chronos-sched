package engine

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"time"
)

// NewRunState constructs the initial empty RunState for a DAG before any events are folded.
func NewRunState(dag *DAG, runID string) *RunState {
	tasks := make(map[string]*TaskState, len(dag.TopoOrder))
	for _, id := range dag.TopoOrder {
		tasks[id] = &TaskState{
			ID:     id,
			Status: TaskStatusPending,
		}
	}
	topoCopy := append([]string(nil), dag.TopoOrder...)
	return &RunState{
		RunID:             runID,
		WorkflowName:      dag.Spec.Name,
		WorkflowHash:      dag.WorkflowHash,
		Status:            RunStatusPending,
		NextFencingToken:  1,
		NextCheckpointNum: 1,
		Tasks:             tasks,
		TopoOrder:         topoCopy,
		Overrides:         make(map[string]map[string]any),
	}
}

// Fold deterministically reconstructs a RunState by applying an ordered event stream.
func Fold(dag *DAG, events []Event) (*RunState, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("cannot fold empty event stream")
	}
	runID := events[len(events)-1].RunID
	state := NewRunState(dag, runID)
	for i, ev := range events {
		expectedSeq := int64(i + 1)
		if ev.Seq != expectedSeq {
			return nil, fmt.Errorf("non-monotonic event seq at index %d: got %d, expected %d", i, ev.Seq, expectedSeq)
		}
		if err := Apply(state, ev); err != nil {
			return nil, fmt.Errorf("apply event seq=%d type=%s: %w", ev.Seq, ev.Type, err)
		}
	}
	return state, nil
}

// Apply mutates state in place according to a single recorded event.
func Apply(state *RunState, ev Event) error {
	state.LastSeq = ev.Seq
	if ev.FencingToken >= state.NextFencingToken {
		state.NextFencingToken = ev.FencingToken + 1
	}

	switch ev.Type {
	case EventRunStarted:
		var p RunStartedPayload
		if len(ev.Payload) > 0 {
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return fmt.Errorf("unmarshal RUN_STARTED: %w", err)
			}
			state.WorkflowName = p.WorkflowName
			state.WorkflowHash = p.WorkflowHash
		}
		state.Status = RunStatusRunning

	case EventTaskLeased:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		var p TaskLeasedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal TASK_LEASED: %w", err)
		}
		ts.Status = TaskStatusLeased
		ts.Attempt = ev.Attempt
		ts.FencingToken = ev.FencingToken
		ts.WorkerID = ev.WorkerID
		ts.LeaseExpiresAtNs = p.LeaseExpiresAtNs

	case EventTaskStarted:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		ts.Status = TaskStatusRunning

	case EventTaskCompleted:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		var p TaskCompletedPayload
		if len(ev.Payload) > 0 {
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return fmt.Errorf("unmarshal TASK_COMPLETED: %w", err)
			}
		}
		ts.Status = TaskStatusCompleted
		ts.LeaseExpiresAtNs = 0
		ts.Output = cloneMap(p.Output)
		ts.CompletedSeq = ev.Seq

	case EventCheckpointSaved:
		var p CheckpointSavedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal CHECKPOINT_SAVED: %w", err)
		}
		state.Checkpoints = append(state.Checkpoints, CheckpointInfo{
			Number: p.CheckpointNum,
			Name:   p.CheckpointName,
			Seq:    ev.Seq,
			TaskID: p.TaskID,
		})
		if p.CheckpointNum >= state.NextCheckpointNum {
			state.NextCheckpointNum = p.CheckpointNum + 1
		}

	case EventTaskFailed:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		var p TaskFailedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal TASK_FAILED: %w", err)
		}
		ts.LastError = p.Error
		ts.WorkerID = ""
		ts.LeaseExpiresAtNs = 0
		if p.WillRetry {
			ts.Status = TaskStatusPending
		} else {
			ts.Status = TaskStatusFailed
		}

	case EventLeaseExpired:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		var p LeaseExpiredPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal LEASE_EXPIRED: %w", err)
		}
		ts.WorkerID = ""
		ts.LeaseExpiresAtNs = 0
		ts.LastError = p.Reason
		if p.WillRetry {
			ts.Status = TaskStatusPending
		} else {
			ts.Status = TaskStatusFailed
		}

	case EventTaskOutcomeAmbiguous:
		ts, ok := state.Tasks[ev.TaskID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTaskNotFound, ev.TaskID)
		}
		ts.AmbiguousCount++

	case EventStaleFencingToken:
		// Audit event only; does not alter task state.

	case EventTimelineForked:
		var p TimelineForkedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal TIMELINE_FORKED: %w", err)
		}
		state.RunID = ev.RunID
		state.ParentRunID = p.ParentRunID
		state.ForkFromSeq = p.ForkFromSeq
		state.Status = RunStatusRunning
		state.FailureReason = ""
		if state.Overrides == nil {
			state.Overrides = make(map[string]map[string]any)
		}
		for taskID, kv := range p.Overrides {
			if state.Overrides[taskID] == nil {
				state.Overrides[taskID] = make(map[string]any)
			}
			maps.Copy(state.Overrides[taskID], kv)
		}
		// Clear inherited active leases on all non-completed tasks so they re-enqueue cleanly.
		for _, id := range state.TopoOrder {
			ts := state.Tasks[id]
			if ts.Status == TaskStatusLeased || ts.Status == TaskStatusRunning {
				ts.Status = TaskStatusPending
				ts.WorkerID = ""
				ts.LeaseExpiresAtNs = 0
			}
		}

	case EventRunCompleted:
		state.Status = RunStatusCompleted

	case EventRunFailed:
		var p RunFailedPayload
		if len(ev.Payload) > 0 {
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return fmt.Errorf("unmarshal RUN_FAILED: %w", err)
			}
			state.FailureReason = p.Reason
		}
		state.Status = RunStatusFailed

	default:
		return fmt.Errorf("unknown event type %q", ev.Type)
	}

	return nil
}

// Step is the pure, clock-injected transition function. It inspects state and cmd
// without mutating state, returning the new events to persist and effects to execute.
func Step(dag *DAG, state *RunState, cmd Command, now time.Time) ([]Event, []Effect, error) {
	nowNs := now.UnixNano()
	nextSeq := state.LastSeq + 1

	switch c := cmd.(type) {
	case CmdStartRun:
		if state.Status != RunStatusPending {
			return nil, nil, fmt.Errorf("%w: run %q already started (status=%s)", ErrInvalidTransition, state.RunID, state.Status)
		}
		payload := mustMarshal(RunStartedPayload{
			WorkflowName: dag.Spec.Name,
			WorkflowHash: dag.WorkflowHash,
		})
		ev := Event{
			RunID:       c.RunID,
			Seq:         1,
			TimestampNs: nowNs,
			Type:        EventRunStarted,
			Payload:     payload,
		}
		return []Event{ev}, nil, nil

	case CmdLeaseNextTask:
		if state.Status != RunStatusRunning {
			return nil, nil, nil
		}
		if c.WorkerID == "" {
			return nil, nil, fmt.Errorf("worker_id is required")
		}
		ttl := c.LeaseTTL
		if ttl <= 0 {
			ttl = DefaultLeaseTTL
		}
		taskID, ok := pickNextReadyTask(dag, state)
		if !ok {
			return nil, nil, nil
		}
		ts := state.Tasks[taskID]
		attempt := ts.Attempt + 1
		token := state.NextFencingToken
		ev := Event{
			RunID:        state.RunID,
			Seq:          nextSeq,
			TimestampNs:  nowNs,
			Type:         EventTaskLeased,
			TaskID:       taskID,
			Attempt:      attempt,
			FencingToken: token,
			WorkerID:     c.WorkerID,
			Payload: mustMarshal(TaskLeasedPayload{
				LeaseExpiresAtNs: now.Add(ttl).UnixNano(),
			}),
		}
		eff := Effect{
			Kind:         EffectDispatchTask,
			RunID:        state.RunID,
			TaskID:       taskID,
			WorkerID:     c.WorkerID,
			Attempt:      attempt,
			FencingToken: token,
		}
		return []Event{ev}, []Effect{eff}, nil

	case CmdMarkTaskStarted:
		if state.Status != RunStatusRunning {
			return nil, nil, ErrRunTerminal
		}
		ts, ok := state.Tasks[c.TaskID]
		if !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrTaskNotFound, c.TaskID)
		}
		if (ts.Status != TaskStatusLeased && ts.Status != TaskStatusRunning) || ts.FencingToken != c.FencingToken || ts.WorkerID != c.WorkerID {
			ev := Event{
				RunID:        state.RunID,
				Seq:          nextSeq,
				TimestampNs:  nowNs,
				Type:         EventStaleFencingToken,
				TaskID:       c.TaskID,
				Attempt:      ts.Attempt,
				FencingToken: c.FencingToken,
				WorkerID:     c.WorkerID,
				Payload: mustMarshal(StaleFencingPayload{
					ExpectedToken: ts.FencingToken,
					ReceivedToken: c.FencingToken,
					Reason:        "task start rejected due to stale fencing token or worker mismatch",
				}),
			}
			return []Event{ev}, nil, ErrStaleFencingToken
		}
		if ts.Status == TaskStatusRunning {
			return nil, nil, nil
		}
		ev := Event{
			RunID:        state.RunID,
			Seq:          nextSeq,
			TimestampNs:  nowNs,
			Type:         EventTaskStarted,
			TaskID:       c.TaskID,
			Attempt:      ts.Attempt,
			FencingToken: c.FencingToken,
			WorkerID:     c.WorkerID,
		}
		return []Event{ev}, nil, nil

	case CmdCompleteTask:
		if state.Status != RunStatusRunning {
			return nil, nil, ErrRunTerminal
		}
		ts, ok := state.Tasks[c.TaskID]
		if !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrTaskNotFound, c.TaskID)
		}
		if (ts.Status != TaskStatusLeased && ts.Status != TaskStatusRunning) || ts.FencingToken != c.FencingToken || ts.WorkerID != c.WorkerID {
			ev := Event{
				RunID:        state.RunID,
				Seq:          nextSeq,
				TimestampNs:  nowNs,
				Type:         EventStaleFencingToken,
				TaskID:       c.TaskID,
				Attempt:      ts.Attempt,
				FencingToken: c.FencingToken,
				WorkerID:     c.WorkerID,
				Payload: mustMarshal(StaleFencingPayload{
					ExpectedToken: ts.FencingToken,
					ReceivedToken: c.FencingToken,
					Reason:        "completion rejected due to stale fencing token or worker mismatch",
				}),
			}
			return []Event{ev}, nil, ErrStaleFencingToken
		}

		spec := dag.TasksByID[c.TaskID]
		completeEv := Event{
			RunID:        state.RunID,
			Seq:          nextSeq,
			TimestampNs:  nowNs,
			Type:         EventTaskCompleted,
			TaskID:       c.TaskID,
			Attempt:      ts.Attempt,
			FencingToken: c.FencingToken,
			WorkerID:     c.WorkerID,
			Payload:      mustMarshal(TaskCompletedPayload{Output: c.Output}),
		}
		cpEv := Event{
			RunID:       state.RunID,
			Seq:         nextSeq + 1,
			TimestampNs: nowNs,
			Type:        EventCheckpointSaved,
			TaskID:      c.TaskID,
			Payload: mustMarshal(CheckpointSavedPayload{
				CheckpointNum:  state.NextCheckpointNum,
				CheckpointName: spec.CheckpointName,
				TaskID:         c.TaskID,
			}),
		}
		events := []Event{completeEv, cpEv}
		var effects []Effect

		allDone := true
		for _, id := range state.TopoOrder {
			if id == c.TaskID {
				continue
			}
			if state.Tasks[id].Status != TaskStatusCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			events = append(events, Event{
				RunID:       state.RunID,
				Seq:         nextSeq + 2,
				TimestampNs: nowNs,
				Type:        EventRunCompleted,
			})
			effects = append(effects, Effect{
				Kind:   EffectRunTerminal,
				RunID:  state.RunID,
				Status: RunStatusCompleted,
			})
		}
		return events, effects, nil

	case CmdFailTask:
		if state.Status != RunStatusRunning {
			return nil, nil, ErrRunTerminal
		}
		ts, ok := state.Tasks[c.TaskID]
		if !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrTaskNotFound, c.TaskID)
		}
		if (ts.Status != TaskStatusLeased && ts.Status != TaskStatusRunning) || ts.FencingToken != c.FencingToken || ts.WorkerID != c.WorkerID {
			ev := Event{
				RunID:        state.RunID,
				Seq:          nextSeq,
				TimestampNs:  nowNs,
				Type:         EventStaleFencingToken,
				TaskID:       c.TaskID,
				Attempt:      ts.Attempt,
				FencingToken: c.FencingToken,
				WorkerID:     c.WorkerID,
				Payload: mustMarshal(StaleFencingPayload{
					ExpectedToken: ts.FencingToken,
					ReceivedToken: c.FencingToken,
					Reason:        "failure report rejected due to stale fencing token or worker mismatch",
				}),
			}
			return []Event{ev}, nil, ErrStaleFencingToken
		}

		spec := dag.TasksByID[c.TaskID]
		willRetry := ts.Attempt <= spec.MaxRetries
		failEv := Event{
			RunID:        state.RunID,
			Seq:          nextSeq,
			TimestampNs:  nowNs,
			Type:         EventTaskFailed,
			TaskID:       c.TaskID,
			Attempt:      ts.Attempt,
			FencingToken: c.FencingToken,
			WorkerID:     c.WorkerID,
			Payload: mustMarshal(TaskFailedPayload{
				Error:     c.ErrMsg,
				WillRetry: willRetry,
			}),
		}
		events := []Event{failEv}
		var effects []Effect
		if !willRetry {
			events = append(events, Event{
				RunID:       state.RunID,
				Seq:         nextSeq + 1,
				TimestampNs: nowNs,
				Type:        EventRunFailed,
				TaskID:      c.TaskID,
				Payload: mustMarshal(RunFailedPayload{
					Reason: fmt.Sprintf("task %s exhausted retries (%d): %s", c.TaskID, spec.MaxRetries, c.ErrMsg),
				}),
			})
			effects = append(effects, Effect{
				Kind:   EffectRunTerminal,
				RunID:  state.RunID,
				Status: RunStatusFailed,
			})
		}
		return events, effects, nil

	case CmdExpireLeases:
		if state.Status != RunStatusRunning {
			return nil, nil, nil
		}
		var events []Event
		var effects []Effect
		seq := nextSeq
		runFailed := false
		for _, id := range state.TopoOrder {
			ts := state.Tasks[id]
			if ts.Status != TaskStatusLeased && ts.Status != TaskStatusRunning {
				continue
			}
			if ts.LeaseExpiresAtNs == 0 || nowNs < ts.LeaseExpiresAtNs {
				continue
			}
			spec := dag.TasksByID[id]
			willRetry := ts.Attempt <= spec.MaxRetries
			events = append(events,
				Event{
					RunID:        state.RunID,
					Seq:          seq,
					TimestampNs:  nowNs,
					Type:         EventLeaseExpired,
					TaskID:       id,
					Attempt:      ts.Attempt,
					FencingToken: ts.FencingToken,
					WorkerID:     ts.WorkerID,
					Payload: mustMarshal(LeaseExpiredPayload{
						Reason:    fmt.Sprintf("worker %s lease expired", ts.WorkerID),
						WillRetry: willRetry,
					}),
				},
				Event{
					RunID:        state.RunID,
					Seq:          seq + 1,
					TimestampNs:  nowNs,
					Type:         EventTaskOutcomeAmbiguous,
					TaskID:       id,
					Attempt:      ts.Attempt,
					FencingToken: ts.FencingToken,
					WorkerID:     ts.WorkerID,
				},
			)
			seq += 2
			if !willRetry && !runFailed {
				runFailed = true
				events = append(events, Event{
					RunID:       state.RunID,
					Seq:         seq,
					TimestampNs: nowNs,
					Type:        EventRunFailed,
					TaskID:      id,
					Payload: mustMarshal(RunFailedPayload{
						Reason: fmt.Sprintf("task %s expired after exhausting retries (%d)", id, spec.MaxRetries),
					}),
				})
				effects = append(effects, Effect{
					Kind:   EffectRunTerminal,
					RunID:  state.RunID,
					Status: RunStatusFailed,
				})
				seq++
			}
		}
		return events, effects, nil

	case CmdForkRun:
		if c.NewRunID == "" {
			return nil, nil, fmt.Errorf("new_run_id is required")
		}
		cp, err := ResolveCheckpoint(state, c.CheckpointSelector)
		if err != nil {
			return nil, nil, err
		}
		// Validate that no override targets a task already completed at <= fork_from_seq.
		for taskID := range c.Overrides {
			ts, ok := state.Tasks[taskID]
			if !ok {
				return nil, nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
			}
			if ts.Status == TaskStatusCompleted && ts.CompletedSeq > 0 && ts.CompletedSeq <= cp.Seq {
				return nil, nil, fmt.Errorf("%w: task %q completed at seq %d (checkpoint %d is at seq %d)", ErrOverrideCompletedTask, taskID, ts.CompletedSeq, cp.Number, cp.Seq)
			}
		}
		ev := Event{
			RunID:       c.NewRunID,
			Seq:         cp.Seq + 1,
			TimestampNs: nowNs,
			Type:        EventTimelineForked,
			Payload: mustMarshal(TimelineForkedPayload{
				ParentRunID:    state.RunID,
				ForkFromSeq:    cp.Seq,
				CheckpointNum:  cp.Number,
				CheckpointName: cp.Name,
				Overrides:      c.Overrides,
			}),
		}
		return []Event{ev}, nil, nil

	default:
		return nil, nil, fmt.Errorf("unsupported command type %T", cmd)
	}
}

// ResolveCheckpoint finds a checkpoint by 1-based numeric ID or optional YAML checkpoint_name.
func ResolveCheckpoint(state *RunState, selector string) (CheckpointInfo, error) {
	if selector == "" {
		return CheckpointInfo{}, fmt.Errorf("%w: empty checkpoint selector", ErrCheckpointNotFound)
	}
	if num, err := strconv.Atoi(selector); err == nil {
		for _, cp := range state.Checkpoints {
			if cp.Number == num {
				return cp, nil
			}
		}
		return CheckpointInfo{}, fmt.Errorf("%w: number %d", ErrCheckpointNotFound, num)
	}
	for _, cp := range state.Checkpoints {
		if cp.Name != "" && cp.Name == selector {
			return cp, nil
		}
	}
	return CheckpointInfo{}, fmt.Errorf("%w: name %q", ErrCheckpointNotFound, selector)
}

// ResolveTaskInput returns the merged input map for taskID: static YAML params
// overlaid with any TIMELINE_FORKED overrides for that task.
func ResolveTaskInput(dag *DAG, state *RunState, taskID string) map[string]any {
	out := make(map[string]any)
	if spec, ok := dag.TasksByID[taskID]; ok {
		maps.Copy(out, spec.Params)
	}
	if ov, ok := state.Overrides[taskID]; ok {
		maps.Copy(out, ov)
	}
	return out
}

func pickNextReadyTask(dag *DAG, state *RunState) (string, bool) {
	for _, id := range state.TopoOrder {
		ts := state.Tasks[id]
		if ts.Status != TaskStatusPending {
			continue
		}
		depsReady := true
		for _, dep := range dag.TasksByID[id].DependsOn {
			if state.Tasks[dep].Status != TaskStatusCompleted {
				depsReady = false
				break
			}
		}
		if depsReady {
			return id, true
		}
	}
	return "", false
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dst := make(map[string]any, len(src))
	maps.Copy(dst, src)
	return dst
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("chronos engine marshal error: %v", err))
	}
	return b
}
