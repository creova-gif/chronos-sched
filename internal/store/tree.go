package store

import (
	"fmt"
	"sort"
	"strings"
)

// FormatBranchTree renders a Git-style ASCII lineage tree of all runs and their
// nested forks, sorted deterministically by (CreatedAtNs, RunID). Orphan parent
// references (where ParentRunID is non-empty but absent from runs) are rendered
// at root level with a [missing parent: <id>] marker.
func FormatBranchTree(runs []RunRecord) string {
	if len(runs) == 0 {
		return ""
	}

	sorted := make([]RunRecord, len(runs))
	copy(sorted, runs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CreatedAtNs != sorted[j].CreatedAtNs {
			return sorted[i].CreatedAtNs < sorted[j].CreatedAtNs
		}
		return sorted[i].RunID < sorted[j].RunID
	})

	byID := make(map[string]RunRecord, len(sorted))
	for _, rec := range sorted {
		byID[rec.RunID] = rec
	}

	var roots []RunRecord
	children := make(map[string][]RunRecord)
	for _, rec := range sorted {
		if rec.ParentRunID == "" {
			roots = append(roots, rec)
			continue
		}
		if _, ok := byID[rec.ParentRunID]; !ok {
			roots = append(roots, rec)
			continue
		}
		children[rec.ParentRunID] = append(children[rec.ParentRunID], rec)
	}

	var lines []string
	visited := make(map[string]bool, len(sorted))

	var render func(rec RunRecord, prefix string, isRoot bool, isLast bool)
	render = func(rec RunRecord, prefix string, isRoot bool, isLast bool) {
		if visited[rec.RunID] {
			return
		}
		visited[rec.RunID] = true

		label := formatRunNode(rec, byID)
		if isRoot {
			lines = append(lines, label)
		} else {
			connector := "|-- "
			if isLast {
				connector = "`-- "
			}
			lines = append(lines, prefix+connector+label)
		}

		kids := children[rec.RunID]
		childPrefix := prefix
		if !isRoot {
			if isLast {
				childPrefix += "    "
			} else {
				childPrefix += "|   "
			}
		}
		for i, kid := range kids {
			render(kid, childPrefix, false, i == len(kids)-1)
		}
	}

	for _, root := range roots {
		render(root, "", true, false)
	}

	// Defensive fallback for cyclic parent references so no run is ever dropped.
	for _, rec := range sorted {
		if !visited[rec.RunID] {
			render(rec, "", true, false)
		}
	}

	return strings.Join(lines, "\n")
}

func formatRunNode(rec RunRecord, byID map[string]RunRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s [%s] %s", rec.RunID, rec.WorkflowName, rec.Status)
	if rec.ParentRunID != "" {
		fmt.Fprintf(&b, " (forked from seq %d)", rec.ForkFromSeq)
		if _, ok := byID[rec.ParentRunID]; !ok {
			fmt.Fprintf(&b, " [missing parent: %s]", rec.ParentRunID)
		}
	}
	return b.String()
}
