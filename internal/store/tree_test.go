package store_test

import (
	"testing"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

func TestFormatBranchTreeEmpty(t *testing.T) {
	if got := store.FormatBranchTree(nil); got != "" {
		t.Fatalf("expected empty string for nil runs, got %q", got)
	}
	if got := store.FormatBranchTree([]store.RunRecord{}); got != "" {
		t.Fatalf("expected empty string for empty runs, got %q", got)
	}
}

func TestFormatBranchTreeSingleRoot(t *testing.T) {
	runs := []store.RunRecord{
		{
			RunID:        "run_001",
			WorkflowName: "etl-daily",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  1000,
		},
	}

	want := "run_001 [etl-daily] COMPLETED"
	if got := store.FormatBranchTree(runs); got != want {
		t.Fatalf("FormatBranchTree mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatBranchTreeMultiHopForksAndDeterministicSort(t *testing.T) {
	// Intentionally supply runs out of order, including a CreatedAtNs tie between
	// run_003 and run_002 to verify tie-breaking by RunID.
	runs := []store.RunRecord{
		{
			RunID:        "run_005",
			ParentRunID:  "run_004",
			ForkFromSeq:  9,
			WorkflowName: "ml-train",
			Status:       engine.RunStatusRunning,
			CreatedAtNs:  5000,
		},
		{
			RunID:        "run_003",
			ParentRunID:  "run_001",
			ForkFromSeq:  4,
			WorkflowName: "ml-train",
			Status:       engine.RunStatusFailed,
			CreatedAtNs:  2000,
		},
		{
			RunID:        "run_004",
			ParentRunID:  "run_002",
			ForkFromSeq:  7,
			WorkflowName: "ml-train",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  3000,
		},
		{
			RunID:        "run_001",
			WorkflowName: "ml-train",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  1000,
		},
		{
			RunID:        "run_002",
			ParentRunID:  "run_001",
			ForkFromSeq:  4,
			WorkflowName: "ml-train",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  2000,
		},
	}

	want := "" +
		"run_001 [ml-train] COMPLETED\n" +
		"|-- run_002 [ml-train] COMPLETED (forked from seq 4)\n" +
		"|   `-- run_004 [ml-train] COMPLETED (forked from seq 7)\n" +
		"|       `-- run_005 [ml-train] RUNNING (forked from seq 9)\n" +
		"`-- run_003 [ml-train] FAILED (forked from seq 4)"

	if got := store.FormatBranchTree(runs); got != want {
		t.Fatalf("FormatBranchTree multi-hop mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatBranchTreeMultipleRoots(t *testing.T) {
	runs := []store.RunRecord{
		{
			RunID:        "run_b",
			WorkflowName: "wf-b",
			Status:       engine.RunStatusRunning,
			CreatedAtNs:  2000,
		},
		{
			RunID:        "run_a_child",
			ParentRunID:  "run_a",
			ForkFromSeq:  3,
			WorkflowName: "wf-a",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  1500,
		},
		{
			RunID:        "run_a",
			WorkflowName: "wf-a",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  1000,
		},
		{
			RunID:        "run_b_child",
			ParentRunID:  "run_b",
			ForkFromSeq:  2,
			WorkflowName: "wf-b",
			Status:       engine.RunStatusFailed,
			CreatedAtNs:  2500,
		},
	}

	want := "" +
		"run_a [wf-a] COMPLETED\n" +
		"`-- run_a_child [wf-a] COMPLETED (forked from seq 3)\n" +
		"run_b [wf-b] RUNNING\n" +
		"`-- run_b_child [wf-b] FAILED (forked from seq 2)"

	if got := store.FormatBranchTree(runs); got != want {
		t.Fatalf("FormatBranchTree multiple roots mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestFormatBranchTreeOrphanParent(t *testing.T) {
	runs := []store.RunRecord{
		{
			RunID:        "run_root",
			WorkflowName: "wf-main",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  1000,
		},
		{
			RunID:        "run_orphan",
			ParentRunID:  "run_deleted",
			ForkFromSeq:  6,
			WorkflowName: "wf-main",
			Status:       engine.RunStatusRunning,
			CreatedAtNs:  2000,
		},
		{
			RunID:        "run_orphan_child",
			ParentRunID:  "run_orphan",
			ForkFromSeq:  8,
			WorkflowName: "wf-main",
			Status:       engine.RunStatusCompleted,
			CreatedAtNs:  3000,
		},
	}

	want := "" +
		"run_root [wf-main] COMPLETED\n" +
		"run_orphan [wf-main] RUNNING (forked from seq 6) [missing parent: run_deleted]\n" +
		"`-- run_orphan_child [wf-main] COMPLETED (forked from seq 8)"

	if got := store.FormatBranchTree(runs); got != want {
		t.Fatalf("FormatBranchTree orphan parent mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}
