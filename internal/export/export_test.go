package export

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

const testWorkflowYAML = `name: etl-export-test
tasks:
  - id: fetch
    handler: http.fetch
    checkpoint_name: after_fetch
    max_retries: 1
  - id: transform
    handler: data.transform
    depends_on: [fetch]
    max_retries: 1
  - id: load
    handler: db.load
    depends_on: [transform]
`

func seedSampleRunAndFork(t *testing.T, st store.JournalStore) (*engine.DAG, string, string) {
	t.Helper()
	ctx := context.Background()

	dag, err := engine.ParseDAG([]byte(testWorkflowYAML))
	if err != nil {
		t.Fatalf("ParseDAG: %v", err)
	}

	baseTime := time.Unix(0, 1_700_000_000_000_000_000).UTC()
	runID := "run-root-1"
	state := engine.NewRunState(dag, runID)

	startEvents, _, err := engine.Step(dag, state, engine.CmdStartRun{RunID: runID}, baseTime)
	if err != nil {
		t.Fatalf("Step StartRun: %v", err)
	}
	for _, ev := range startEvents {
		if err := engine.Apply(state, ev); err != nil {
			t.Fatalf("Apply start: %v", err)
		}
	}

	rec := store.RunRecord{
		RunID:        runID,
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: testWorkflowYAML,
		Status:       state.Status,
		CreatedAtNs:  baseTime.UnixNano(),
	}
	if err := st.CreateRun(ctx, rec, startEvents); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	stepAndAppend := func(cmd engine.Command, ts time.Time) {
		t.Helper()
		expectedLast := state.LastSeq
		evs, _, err := engine.Step(dag, state, cmd, ts)
		if err != nil {
			t.Fatalf("Step %T: %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(state, ev); err != nil {
				t.Fatalf("Apply %s: %v", ev.Type, err)
			}
		}
		if err := st.Append(ctx, runID, expectedLast, evs, state.Status); err != nil {
			t.Fatalf("Append %T: %v", cmd, err)
		}
	}

	// Execute fetch -> transform -> load on root run.
	stepAndAppend(engine.CmdLeaseNextTask{WorkerID: "worker-a", LeaseTTL: 3 * time.Second}, baseTime.Add(1*time.Millisecond))
	stepAndAppend(engine.CmdMarkTaskStarted{TaskID: "fetch", WorkerID: "worker-a", FencingToken: 1}, baseTime.Add(2*time.Millisecond))
	stepAndAppend(engine.CmdCompleteTask{TaskID: "fetch", WorkerID: "worker-a", FencingToken: 1, Output: map[string]any{"rows": 42}}, baseTime.Add(5*time.Millisecond))

	// Fork from checkpoint after_fetch before completing transform/load.
	forkRunID := "run-fork-1"
	forkEvents, _, err := engine.Step(dag, state, engine.CmdForkRun{
		NewRunID:           forkRunID,
		CheckpointSelector: "after_fetch",
		Overrides:          map[string]map[string]any{"transform": {"mode": "strict"}},
	}, baseTime.Add(6*time.Millisecond))
	if err != nil {
		t.Fatalf("Step ForkRun: %v", err)
	}
	forkRec := store.RunRecord{
		RunID:        forkRunID,
		ParentRunID:  runID,
		ForkFromSeq:  state.Checkpoints[0].Seq,
		WorkflowName: dag.Spec.Name,
		WorkflowHash: dag.WorkflowHash,
		WorkflowYAML: testWorkflowYAML,
		Status:       engine.RunStatusRunning,
		CreatedAtNs:  baseTime.Add(6 * time.Millisecond).UnixNano(),
	}
	if err := st.CreateRun(ctx, forkRec, forkEvents); err != nil {
		t.Fatalf("CreateRun fork: %v", err)
	}

	// Finish root run.
	stepAndAppend(engine.CmdLeaseNextTask{WorkerID: "worker-b", LeaseTTL: 3 * time.Second}, baseTime.Add(7*time.Millisecond))
	stepAndAppend(engine.CmdCompleteTask{TaskID: "transform", WorkerID: "worker-b", FencingToken: 2, Output: map[string]any{"clean": true}}, baseTime.Add(9*time.Millisecond))
	stepAndAppend(engine.CmdLeaseNextTask{WorkerID: "worker-a", LeaseTTL: 3 * time.Second}, baseTime.Add(10*time.Millisecond))
	stepAndAppend(engine.CmdCompleteTask{TaskID: "load", WorkerID: "worker-a", FencingToken: 3, Output: map[string]any{"inserted": 42}}, baseTime.Add(12*time.Millisecond))

	// Finish fork run.
	forkLoaded, err := st.LoadRunEvents(ctx, forkRunID)
	if err != nil {
		t.Fatalf("LoadRunEvents fork: %v", err)
	}
	forkState, err := engine.Fold(dag, forkLoaded)
	if err != nil {
		t.Fatalf("Fold fork: %v", err)
	}
	forkStepAndAppend := func(cmd engine.Command, ts time.Time) {
		t.Helper()
		expectedLast := forkState.LastSeq
		evs, _, err := engine.Step(dag, forkState, cmd, ts)
		if err != nil {
			t.Fatalf("fork Step %T: %v", cmd, err)
		}
		for _, ev := range evs {
			if err := engine.Apply(forkState, ev); err != nil {
				t.Fatalf("fork Apply %s: %v", ev.Type, err)
			}
		}
		if err := st.Append(ctx, forkRunID, expectedLast, evs, forkState.Status); err != nil {
			t.Fatalf("fork Append %T: %v", cmd, err)
		}
	}
	forkStepAndAppend(engine.CmdLeaseNextTask{WorkerID: "worker-c", LeaseTTL: 3 * time.Second}, baseTime.Add(15*time.Millisecond))
	forkStepAndAppend(engine.CmdCompleteTask{TaskID: "transform", WorkerID: "worker-c", FencingToken: 2, Output: map[string]any{"clean": true, "mode": "strict"}}, baseTime.Add(18*time.Millisecond))
	forkStepAndAppend(engine.CmdLeaseNextTask{WorkerID: "worker-c", LeaseTTL: 3 * time.Second}, baseTime.Add(19*time.Millisecond))
	forkStepAndAppend(engine.CmdCompleteTask{TaskID: "load", WorkerID: "worker-c", FencingToken: 3, Output: map[string]any{"inserted": 42}}, baseTime.Add(22*time.Millisecond))

	return dag, runID, forkRunID
}

func TestExportImportBundleRoundTrip(t *testing.T) {
	ctx := context.Background()
	srcDB, err := sqlite.Open(filepath.Join(t.TempDir(), "src.db"))
	if err != nil {
		t.Fatalf("open src db: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	dag, rootRunID, forkRunID := seedSampleRunAndFork(t, srcDB)

	for _, tc := range []struct {
		name          string
		sourceRunID   string
		overrideRunID string
	}{
		{name: "root_run_preserve_id", sourceRunID: rootRunID, overrideRunID: ""},
		{name: "forked_run_flattened_override_id", sourceRunID: forkRunID, overrideRunID: "imported-fork-99"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origEvents, err := srcDB.LoadRunEvents(ctx, tc.sourceRunID)
			if err != nil {
				t.Fatalf("LoadRunEvents orig: %v", err)
			}
			origReport, err := engine.VerifyReplay(dag, origEvents)
			if err != nil {
				t.Fatalf("VerifyReplay orig: %v", err)
			}

			bundle, err := ExportBundle(ctx, srcDB, tc.sourceRunID)
			if err != nil {
				t.Fatalf("ExportBundle: %v", err)
			}

			// Round-trip through indented JSON to verify canonical checksum stability.
			rawJSON, err := json.MarshalIndent(bundle, "", "  ")
			if err != nil {
				t.Fatalf("MarshalIndent: %v", err)
			}

			var decoded FlightBundle
			if err := json.Unmarshal(rawJSON, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}

			if err := VerifyBundle(&decoded); err != nil {
				t.Fatalf("VerifyBundle: %v", err)
			}

			dstDB, err := sqlite.Open(filepath.Join(t.TempDir(), "dst.db"))
			if err != nil {
				t.Fatalf("open dst db: %v", err)
			}
			defer func() { _ = dstDB.Close() }()

			importedID, err := ImportBundle(ctx, dstDB, &decoded, tc.overrideRunID)
			if err != nil {
				t.Fatalf("ImportBundle: %v", err)
			}
			wantID := tc.sourceRunID
			if tc.overrideRunID != "" {
				wantID = tc.overrideRunID
			}
			if importedID != wantID {
				t.Fatalf("importedID=%q, want %q", importedID, wantID)
			}

			importedRec, err := dstDB.GetRun(ctx, importedID)
			if err != nil {
				t.Fatalf("GetRun imported: %v", err)
			}
			if importedRec.ParentRunID != "" || importedRec.ForkFromSeq != 0 {
				t.Fatalf("expected cleared lineage on import, got parent=%q fork_from_seq=%d", importedRec.ParentRunID, importedRec.ForkFromSeq)
			}

			importedEvents, err := dstDB.LoadRunEvents(ctx, importedID)
			if err != nil {
				t.Fatalf("LoadRunEvents imported: %v", err)
			}
			for i, ev := range importedEvents {
				if ev.RunID != importedID {
					t.Fatalf("event[%d].RunID=%q, want %q", i, ev.RunID, importedID)
				}
			}

			importedDAG, err := engine.ParseDAG([]byte(importedRec.WorkflowYAML))
			if err != nil {
				t.Fatalf("ParseDAG imported: %v", err)
			}
			importedReport, err := engine.VerifyReplay(importedDAG, importedEvents)
			if err != nil {
				t.Fatalf("VerifyReplay imported: %v", err)
			}

			if importedReport.EventsReplayed != origReport.EventsReplayed ||
				importedReport.Checkpoints != origReport.Checkpoints ||
				importedReport.FinalStatus != origReport.FinalStatus ||
				importedReport.CompletedTasks != origReport.CompletedTasks ||
				importedReport.TotalTasks != origReport.TotalTasks ||
				importedReport.AmbiguousEvents != origReport.AmbiguousEvents {
				t.Fatalf("replay report mismatch: imported=%+v orig=%+v", importedReport, origReport)
			}
		})
	}
}

func TestVerifyBundleTamperedChecksum(t *testing.T) {
	ctx := context.Background()
	srcDB, err := sqlite.Open(filepath.Join(t.TempDir(), "src.db"))
	if err != nil {
		t.Fatalf("open src db: %v", err)
	}
	defer func() { _ = srcDB.Close() }()

	_, rootRunID, _ := seedSampleRunAndFork(t, srcDB)
	bundle, err := ExportBundle(ctx, srcDB, rootRunID)
	if err != nil {
		t.Fatalf("ExportBundle: %v", err)
	}

	if err := VerifyBundle(nil); !errors.Is(err, ErrNilBundle) {
		t.Fatalf("expected ErrNilBundle, got %v", err)
	}

	badVersion := *bundle
	badVersion.FormatVersion = "chronos.flight-bundle/v999"
	if err := VerifyBundle(&badVersion); !errors.Is(err, ErrInvalidFormatVersion) {
		t.Fatalf("expected ErrInvalidFormatVersion, got %v", err)
	}

	// Tamper with an event field after export.
	tampered := *bundle
	tampered.Events = append([]engine.Event(nil), bundle.Events...)
	tampered.Events[1].WorkerID = "evil-worker"

	if err := VerifyBundle(&tampered); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch for tampered event, got %v", err)
	}

	// ImportBundle must also reject the tampered bundle without writing to dstDB.
	dstDB, err := sqlite.Open(filepath.Join(t.TempDir(), "dst.db"))
	if err != nil {
		t.Fatalf("open dst db: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	if _, err := ImportBundle(ctx, dstDB, &tampered, ""); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ImportBundle to return ErrChecksumMismatch, got %v", err)
	}
	runs, err := dstDB.ListRuns(ctx)
	if err != nil {
		t.Fatalf("ListRuns dst: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs persisted after failed import, got %d", len(runs))
	}
}

func TestToChromeTrace(t *testing.T) {
	emptyBytes, err := ToChromeTrace(nil)
	if err != nil {
		t.Fatalf("ToChromeTrace(nil): %v", err)
	}
	var emptyDoc map[string][]map[string]any
	if err := json.Unmarshal(emptyBytes, &emptyDoc); err != nil {
		t.Fatalf("unmarshal empty trace: %v", err)
	}
	if len(emptyDoc["traceEvents"]) != 0 {
		t.Fatalf("expected 0 traceEvents for empty input, got %d", len(emptyDoc["traceEvents"]))
	}

	baseNs := int64(1_700_000_000_000_000_000)
	ms := func(m int64) int64 { return baseNs + m*int64(time.Millisecond) }

	events := []engine.Event{
		{RunID: "r1", Seq: 1, TimestampNs: ms(0), Type: engine.EventRunStarted},
		{RunID: "r1", Seq: 2, TimestampNs: ms(10), Type: engine.EventTaskLeased, TaskID: "fetch", Attempt: 1, FencingToken: 1, WorkerID: "worker-b"},
		{RunID: "r1", Seq: 3, TimestampNs: ms(12), Type: engine.EventTaskStarted, TaskID: "fetch", Attempt: 1, FencingToken: 1, WorkerID: "worker-b"},
		{RunID: "r1", Seq: 4, TimestampNs: ms(50), Type: engine.EventLeaseExpired, TaskID: "fetch", Attempt: 1, FencingToken: 1, WorkerID: "worker-b", Payload: json.RawMessage(`{"reason":"worker-b lease expired","will_retry":true}`)},
		{RunID: "r1", Seq: 5, TimestampNs: ms(50), Type: engine.EventTaskOutcomeAmbiguous, TaskID: "fetch", Attempt: 1, FencingToken: 1, WorkerID: "worker-b"},
		{RunID: "r1", Seq: 6, TimestampNs: ms(55), Type: engine.EventStaleFencingToken, TaskID: "fetch", Attempt: 1, FencingToken: 1, WorkerID: "worker-b", Payload: json.RawMessage(`{"expected_token":2,"received_token":1}`)},
		{RunID: "r1", Seq: 7, TimestampNs: ms(60), Type: engine.EventTaskLeased, TaskID: "fetch", Attempt: 2, FencingToken: 2, WorkerID: "worker-a"},
		{RunID: "r1", Seq: 8, TimestampNs: ms(62), Type: engine.EventTaskStarted, TaskID: "fetch", Attempt: 2, FencingToken: 2, WorkerID: "worker-a"},
		{RunID: "r1", Seq: 9, TimestampNs: ms(95), Type: engine.EventTaskCompleted, TaskID: "fetch", Attempt: 2, FencingToken: 2, WorkerID: "worker-a"},
		{RunID: "r1", Seq: 10, TimestampNs: ms(95), Type: engine.EventCheckpointSaved, TaskID: "fetch", Payload: json.RawMessage(`{"checkpoint_num":1,"checkpoint_name":"after_fetch","task_id":"fetch"}`)},
		{RunID: "r1", Seq: 11, TimestampNs: ms(100), Type: engine.EventTimelineForked, Payload: json.RawMessage(`{"parent_run_id":"r0","fork_from_seq":10,"checkpoint_num":1}`)},
		{RunID: "r1", Seq: 12, TimestampNs: ms(110), Type: engine.EventTimelineMerged, Payload: json.RawMessage(`{"base_run_id":"r0","branch_run_id":"r1","merge_from_seq":11}`)},
		{RunID: "r1", Seq: 13, TimestampNs: ms(120), Type: engine.EventTaskLeased, TaskID: "transform", Attempt: 1, FencingToken: 3, WorkerID: "worker-a"},
		{RunID: "r1", Seq: 14, TimestampNs: ms(145), Type: engine.EventTaskFailed, TaskID: "transform", Attempt: 1, FencingToken: 3, WorkerID: "worker-a", Payload: json.RawMessage(`{"error":"bad input","will_retry":false}`)},
	}

	raw, err := ToChromeTrace(events)
	if err != nil {
		t.Fatalf("ToChromeTrace: %v", err)
	}

	var parsed struct {
		TraceEvents []map[string]any `json:"traceEvents"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal chrome trace JSON: %v", err)
	}

	threadNames := make(map[string]int)
	var xEvents []map[string]any
	instantEvents := make(map[string]map[string]any)

	for _, ev := range parsed.TraceEvents {
		if int(ev["pid"].(float64)) != 1 {
			t.Fatalf("expected pid=1, got %v", ev["pid"])
		}
		ph, _ := ev["ph"].(string)
		switch ph {
		case "M":
			if ev["name"] != "thread_name" {
				t.Fatalf("unexpected metadata event name: %v", ev["name"])
			}
			args := ev["args"].(map[string]any)
			threadNames[args["name"].(string)] = int(ev["tid"].(float64))
		case "X":
			xEvents = append(xEvents, ev)
		case "i":
			instantEvents[ev["name"].(string)] = ev
		default:
			t.Fatalf("unexpected trace phase %q", ph)
		}
	}

	// Verify deterministic worker thread_name lanes (worker-a -> 1, worker-b -> 2).
	if threadNames["coordinator"] != 0 {
		t.Fatalf("expected coordinator tid=0, got %v", threadNames)
	}
	if threadNames["worker-a"] != 1 || threadNames["worker-b"] != 2 {
		t.Fatalf("expected deterministic worker tids worker-a=1 worker-b=2, got %v", threadNames)
	}

	// Verify 3 complete spans: LEASE_EXPIRED (40ms), COMPLETED (35ms), FAILED (25ms).
	if len(xEvents) != 3 {
		t.Fatalf("expected 3 X span events, got %d: %+v", len(xEvents), xEvents)
	}
	expectedSpans := []struct {
		name   string
		tid    int
		durUs  float64
		status string
		att    float64
		token  float64
	}{
		{name: "fetch", tid: 2, durUs: 40_000, status: "LEASE_EXPIRED", att: 1, token: 1},
		{name: "fetch", tid: 1, durUs: 35_000, status: "COMPLETED", att: 2, token: 2},
		{name: "transform", tid: 1, durUs: 25_000, status: "FAILED", att: 1, token: 3},
	}
	for i, want := range expectedSpans {
		got := xEvents[i]
		if got["name"] != want.name {
			t.Fatalf("span[%d] name=%v, want %s", i, got["name"], want.name)
		}
		if int(got["tid"].(float64)) != want.tid {
			t.Fatalf("span[%d] tid=%v, want %d", i, got["tid"], want.tid)
		}
		dur := got["dur"].(float64)
		if dur < 0 || dur != want.durUs {
			t.Fatalf("span[%d] dur=%v, want %v", i, dur, want.durUs)
		}
		args := got["args"].(map[string]any)
		if args["status"] != want.status || args["attempt"].(float64) != want.att || args["fencing_token"].(float64) != want.token {
			t.Fatalf("span[%d] args=%+v, want status=%s att=%v token=%v", i, args, want.status, want.att, want.token)
		}
	}

	// Verify all 5 instant marker types are emitted.
	for _, wantInstant := range []string{
		string(engine.EventCheckpointSaved),
		string(engine.EventTaskOutcomeAmbiguous),
		string(engine.EventStaleFencingToken),
		string(engine.EventTimelineForked),
		string(engine.EventTimelineMerged),
	} {
		ev, ok := instantEvents[wantInstant]
		if !ok {
			t.Fatalf("missing instant marker event %q", wantInstant)
		}
		if _, hasTs := ev["ts"]; !hasTs {
			t.Fatalf("instant event %q missing ts", wantInstant)
		}
	}
}
