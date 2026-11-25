package engine

import (
	"cmp"
	"fmt"
	"slices"
)

// EventFilter specifies optional criteria for querying an event journal.
// Zero-valued fields are ignored.
type EventFilter struct {
	TaskID    string
	WorkerID  string
	EventType EventType
	MinSeq    int64
	MaxSeq    int64
}

// FilterEvents returns all events matching f in chronological Seq order.
func FilterEvents(events []Event, f EventFilter) []Event {
	var out []Event
	for _, ev := range events {
		if f.TaskID != "" && ev.TaskID != f.TaskID {
			continue
		}
		if f.WorkerID != "" && ev.WorkerID != f.WorkerID {
			continue
		}
		if f.EventType != "" && ev.Type != f.EventType {
			continue
		}
		if f.MinSeq > 0 && ev.Seq < f.MinSeq {
			continue
		}
		if f.MaxSeq > 0 && ev.Seq > f.MaxSeq {
			continue
		}
		out = append(out, ev)
	}
	slices.SortStableFunc(out, func(a, b Event) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return out
}

// StateAtCheckpoint reconstructs the exact historical RunState at the checkpoint
// identified by checkpointSelector (1-based checkpoint number or checkpoint_name).
func StateAtCheckpoint(dag *DAG, events []Event, checkpointSelector string) (*RunState, CheckpointInfo, error) {
	if dag == nil {
		return nil, CheckpointInfo{}, fmt.Errorf("dag is required")
	}
	fullState, err := Fold(dag, events)
	if err != nil {
		return nil, CheckpointInfo{}, err
	}
	cp, err := ResolveCheckpoint(fullState, checkpointSelector)
	if err != nil {
		return nil, CheckpointInfo{}, err
	}
	cpIdx := -1
	for i, ev := range events {
		if ev.Seq == cp.Seq {
			cpIdx = i
			break
		}
	}
	if cpIdx < 0 {
		return nil, CheckpointInfo{}, fmt.Errorf("%w: checkpoint seq %d not found in event stream", ErrCheckpointNotFound, cp.Seq)
	}
	stateAtCp, err := Fold(dag, events[:cpIdx+1])
	if err != nil {
		return nil, CheckpointInfo{}, fmt.Errorf("fold prefix up to checkpoint %d (seq=%d): %w", cp.Number, cp.Seq, err)
	}
	return stateAtCp, cp, nil
}

// StateAtSeq reconstructs the historical RunState by folding the event stream
// prefix up to and including sequence number seq (Seq <= seq).
func StateAtSeq(dag *DAG, events []Event, seq int64) (*RunState, error) {
	if dag == nil {
		return nil, fmt.Errorf("dag is required")
	}
	if seq <= 0 {
		return nil, fmt.Errorf("seq must be > 0, got %d", seq)
	}
	if len(events) == 0 {
		return Fold(dag, events)
	}
	end := 0
	for end < len(events) && events[end].Seq <= seq {
		end++
	}
	return Fold(dag, events[:end])
}
