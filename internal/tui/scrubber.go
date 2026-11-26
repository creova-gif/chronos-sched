package tui

import (
	"encoding/json"
	"fmt"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

// ScrubSession tracks a time-travel cursor over an ordered slice of workflow events.
type ScrubSession struct {
	events []engine.Event
	cursor int
}

// NewScrubSession creates a ScrubSession positioned at the latest event.
func NewScrubSession(events []engine.Event) ScrubSession {
	cursor := 0
	if len(events) > 0 {
		cursor = len(events) - 1
	}
	return ScrubSession{
		events: events,
		cursor: cursor,
	}
}

// Cursor returns the current event index in 0..len(events)-1.
func (s ScrubSession) Cursor() int {
	return s.cursor
}

// Step moves the cursor by delta, clamping to 0..len(events)-1.
func (s *ScrubSession) Step(delta int) int {
	s.cursor = StepScrubber(s.events, s.cursor, delta)
	return s.cursor
}

// CurrentEvent returns the event at the current cursor, if any.
func (s ScrubSession) CurrentEvent() (engine.Event, bool) {
	if len(s.events) == 0 || s.cursor < 0 || s.cursor >= len(s.events) {
		return engine.Event{}, false
	}
	return s.events[s.cursor], true
}

// ComputeDisplayState reconstructs the historical RunState at the current cursor.
func (s ScrubSession) ComputeDisplayState(dag *engine.DAG) (*engine.RunState, error) {
	ev, ok := s.CurrentEvent()
	if !ok {
		return nil, fmt.Errorf("no events available to scrub")
	}
	return engine.StateAtSeq(dag, s.events, ev.Seq)
}

// StepScrubber advances cursor by delta and clamps it within 0..len(events)-1.
func StepScrubber(events []engine.Event, cursor int, delta int) int {
	if len(events) == 0 {
		return 0
	}
	next := cursor + delta
	if next < 0 {
		return 0
	}
	if next >= len(events) {
		return len(events) - 1
	}
	return next
}

// RenderScrubBanner formats the status banner shown while in time-travel scrub mode.
func RenderScrubBanner(ev engine.Event, idx, total int) string {
	_ = idx
	return fmt.Sprintf("SCRUB MODE: seq %d/%d (%s) — use [<] / [>] or left/right, [s/esc] Exit", ev.Seq, total, ev.Type)
}

// FindCheckpointIndex returns the index of the CHECKPOINT_SAVED event matching cpNum, or -1.
func FindCheckpointIndex(events []engine.Event, cpNum int) int {
	for i, ev := range events {
		if ev.Type != engine.EventCheckpointSaved || len(ev.Payload) == 0 {
			continue
		}
		var payload engine.CheckpointSavedPayload
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			continue
		}
		if payload.CheckpointNum == cpNum {
			return i
		}
	}
	return -1
}
