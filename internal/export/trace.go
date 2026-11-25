package export

import (
	"encoding/json"
	"sort"

	"github.com/creova-gif/chronos-sched/internal/engine"
)

const (
	traceProcessID    = 1
	coordinatorTID    = 0
	coordinatorThread = "coordinator"
)

// TraceFile is the top-level Chrome Trace Event Format envelope compatible with
// chrome://tracing and https://ui.perfetto.dev.
type TraceFile struct {
	TraceEvents []TraceEvent `json:"traceEvents"`
}

// TraceEvent represents a single Chrome Trace event entry (M, X, or i).
type TraceEvent struct {
	Name string         `json:"name"`
	Cat  string         `json:"cat,omitempty"`
	Ph   string         `json:"ph"`
	Ts   *int64         `json:"ts,omitempty"`
	Dur  *int64         `json:"dur,omitempty"`
	Pid  int            `json:"pid"`
	Tid  int            `json:"tid"`
	S    string         `json:"s,omitempty"`
	Args map[string]any `json:"args,omitempty"`
}

type spanKey struct {
	taskID       string
	attempt      int
	fencingToken int64
}

type openSpan struct {
	taskID       string
	attempt      int
	fencingToken int64
	workerID     string
	startNs      int64
}

// ToChromeTrace converts an ordered ChronosSched event stream into standard
// Chrome Trace Event Format JSON ({"traceEvents": [...]}) for inspection in
// chrome://tracing and Perfetto UI.
func ToChromeTrace(events []engine.Event) ([]byte, error) {
	if len(events) == 0 {
		return json.Marshal(TraceFile{TraceEvents: []TraceEvent{}})
	}

	baseNs := events[0].TimestampNs
	maxNs := events[0].TimestampNs
	workerSet := make(map[string]struct{})
	for _, ev := range events {
		if ev.TimestampNs < baseNs {
			baseNs = ev.TimestampNs
		}
		if ev.TimestampNs > maxNs {
			maxNs = ev.TimestampNs
		}
		if ev.WorkerID != "" {
			workerSet[ev.WorkerID] = struct{}{}
		}
	}

	workers := make([]string, 0, len(workerSet))
	for w := range workerSet {
		workers = append(workers, w)
	}
	sort.Strings(workers)

	workerTID := make(map[string]int, len(workers))
	traceEvents := make([]TraceEvent, 0, len(events)+len(workers)+1)

	traceEvents = append(traceEvents, TraceEvent{
		Name: "thread_name",
		Ph:   "M",
		Pid:  traceProcessID,
		Tid:  coordinatorTID,
		Args: map[string]any{"name": coordinatorThread},
	})
	for i, w := range workers {
		tid := i + 1
		workerTID[w] = tid
		traceEvents = append(traceEvents, TraceEvent{
			Name: "thread_name",
			Ph:   "M",
			Pid:  traceProcessID,
			Tid:  tid,
			Args: map[string]any{"name": w},
		})
	}

	resolveTID := func(workerID string) int {
		if tid, ok := workerTID[workerID]; ok {
			return tid
		}
		return coordinatorTID
	}

	openSpans := make(map[spanKey]*openSpan)

	for _, ev := range events {
		switch ev.Type {
		case engine.EventTaskLeased:
			k := spanKey{taskID: ev.TaskID, attempt: ev.Attempt, fencingToken: ev.FencingToken}
			openSpans[k] = &openSpan{
				taskID:       ev.TaskID,
				attempt:      ev.Attempt,
				fencingToken: ev.FencingToken,
				workerID:     ev.WorkerID,
				startNs:      ev.TimestampNs,
			}

		case engine.EventTaskStarted:
			k := spanKey{taskID: ev.TaskID, attempt: ev.Attempt, fencingToken: ev.FencingToken}
			if sp, ok := openSpans[k]; ok {
				if sp.workerID == "" && ev.WorkerID != "" {
					sp.workerID = ev.WorkerID
				}
			} else {
				openSpans[k] = &openSpan{
					taskID:       ev.TaskID,
					attempt:      ev.Attempt,
					fencingToken: ev.FencingToken,
					workerID:     ev.WorkerID,
					startNs:      ev.TimestampNs,
				}
			}

		case engine.EventTaskCompleted, engine.EventTaskFailed, engine.EventLeaseExpired:
			status := spanStatusForEvent(ev.Type)
			sp, k, found := findOpenSpan(openSpans, ev)
			startNs := ev.TimestampNs
			workerID := ev.WorkerID
			attempt := ev.Attempt
			fencingToken := ev.FencingToken
			if found {
				startNs = sp.startNs
				if workerID == "" {
					workerID = sp.workerID
				}
				if attempt == 0 {
					attempt = sp.attempt
				}
				if fencingToken == 0 {
					fencingToken = sp.fencingToken
				}
				delete(openSpans, k)
			}

			tsUs := toMicros(startNs - baseNs)
			durUs := toMicros(ev.TimestampNs - startNs)
			args := map[string]any{
				"task_id":       ev.TaskID,
				"attempt":       attempt,
				"fencing_token": fencingToken,
				"status":        status,
			}
			if workerID != "" {
				args["worker_id"] = workerID
			}
			attachPayloadArgs(args, ev)

			traceEvents = append(traceEvents, TraceEvent{
				Name: ev.TaskID,
				Cat:  "task",
				Ph:   "X",
				Ts:   int64Ptr(tsUs),
				Dur:  int64Ptr(durUs),
				Pid:  traceProcessID,
				Tid:  resolveTID(workerID),
				Args: args,
			})

		case engine.EventCheckpointSaved,
			engine.EventTaskOutcomeAmbiguous,
			engine.EventStaleFencingToken,
			engine.EventTimelineForked,
			engine.EventTimelineMerged:
			tsUs := toMicros(ev.TimestampNs - baseNs)
			scope := "g"
			if ev.WorkerID != "" {
				scope = "t"
			}
			args := map[string]any{
				"seq":        ev.Seq,
				"event_type": string(ev.Type),
			}
			if ev.TaskID != "" {
				args["task_id"] = ev.TaskID
			}
			if ev.Attempt != 0 {
				args["attempt"] = ev.Attempt
			}
			if ev.FencingToken != 0 {
				args["fencing_token"] = ev.FencingToken
			}
			if ev.WorkerID != "" {
				args["worker_id"] = ev.WorkerID
			}
			attachPayloadArgs(args, ev)

			traceEvents = append(traceEvents, TraceEvent{
				Name: string(ev.Type),
				Cat:  instantCategoryForEvent(ev.Type),
				Ph:   "i",
				Ts:   int64Ptr(tsUs),
				Pid:  traceProcessID,
				Tid:  resolveTID(ev.WorkerID),
				S:    scope,
				Args: args,
			})
		}
	}

	// Close any in-flight spans deterministically at maxNs so partial runs still render.
	if len(openSpans) > 0 {
		remaining := make([]*openSpan, 0, len(openSpans))
		for _, sp := range openSpans {
			remaining = append(remaining, sp)
		}
		sort.Slice(remaining, func(i, j int) bool {
			if remaining[i].startNs != remaining[j].startNs {
				return remaining[i].startNs < remaining[j].startNs
			}
			if remaining[i].taskID != remaining[j].taskID {
				return remaining[i].taskID < remaining[j].taskID
			}
			return remaining[i].attempt < remaining[j].attempt
		})
		for _, sp := range remaining {
			tsUs := toMicros(sp.startNs - baseNs)
			durUs := toMicros(maxNs - sp.startNs)
			args := map[string]any{
				"task_id":       sp.taskID,
				"attempt":       sp.attempt,
				"fencing_token": sp.fencingToken,
				"status":        "IN_FLIGHT",
			}
			if sp.workerID != "" {
				args["worker_id"] = sp.workerID
			}
			traceEvents = append(traceEvents, TraceEvent{
				Name: sp.taskID,
				Cat:  "task",
				Ph:   "X",
				Ts:   int64Ptr(tsUs),
				Dur:  int64Ptr(durUs),
				Pid:  traceProcessID,
				Tid:  resolveTID(sp.workerID),
				Args: args,
			})
		}
	}

	return json.Marshal(TraceFile{TraceEvents: traceEvents})
}

func findOpenSpan(openSpans map[spanKey]*openSpan, ev engine.Event) (*openSpan, spanKey, bool) {
	exact := spanKey{taskID: ev.TaskID, attempt: ev.Attempt, fencingToken: ev.FencingToken}
	if sp, ok := openSpans[exact]; ok {
		return sp, exact, true
	}
	for k, sp := range openSpans {
		if k.taskID == ev.TaskID && (ev.FencingToken == 0 || k.fencingToken == ev.FencingToken) {
			return sp, k, true
		}
	}
	return nil, spanKey{}, false
}

func spanStatusForEvent(t engine.EventType) string {
	switch t {
	case engine.EventTaskCompleted:
		return "COMPLETED"
	case engine.EventTaskFailed:
		return "FAILED"
	case engine.EventLeaseExpired:
		return "LEASE_EXPIRED"
	default:
		return string(t)
	}
}

func instantCategoryForEvent(t engine.EventType) string {
	switch t {
	case engine.EventCheckpointSaved:
		return "checkpoint"
	case engine.EventTaskOutcomeAmbiguous, engine.EventStaleFencingToken:
		return "fault"
	case engine.EventTimelineForked, engine.EventTimelineMerged:
		return "timeline"
	default:
		return "lifecycle"
	}
}

func attachPayloadArgs(args map[string]any, ev engine.Event) {
	if len(ev.Payload) == 0 {
		return
	}
	var decoded map[string]any
	if err := json.Unmarshal(ev.Payload, &decoded); err != nil {
		return
	}
	for k, v := range decoded {
		if _, exists := args[k]; !exists {
			args[k] = v
		}
	}
}

func toMicros(deltaNs int64) int64 {
	if deltaNs <= 0 {
		return 0
	}
	return deltaNs / 1000
}

func int64Ptr(v int64) *int64 {
	return &v
}
