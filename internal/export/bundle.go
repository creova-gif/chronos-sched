package export

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
)

const FormatVersionV1 = "chronos.flight-bundle/v1"

var (
	ErrNilBundle            = errors.New("flight bundle is nil")
	ErrInvalidFormatVersion = errors.New("unsupported flight bundle format version")
	ErrChecksumMismatch     = errors.New("flight bundle checksum mismatch")
)

// FlightBundle is a portable, tamper-evident snapshot of a single run and its
// full resolved event stream (1..N), suitable for offline replay or import into
// another JournalStore.
type FlightBundle struct {
	FormatVersion  string          `json:"format_version"`
	ExportedAtNs   int64           `json:"exported_at_ns"`
	Run            store.RunRecord `json:"run"`
	Events         []engine.Event  `json:"events"`
	ChecksumSHA256 string          `json:"checksum_sha256"`
}

type canonicalBundlePayload struct {
	Run    store.RunRecord `json:"run"`
	Events []engine.Event  `json:"events"`
}

// ExportBundle loads a run record and its full resolved lineage event stream
// from st, then signs the canonical JSON of (Run, Events) with SHA-256.
func ExportBundle(ctx context.Context, st store.JournalStore, runID string) (*FlightBundle, error) {
	if st == nil {
		return nil, fmt.Errorf("journal store is required")
	}
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}

	rec, err := st.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("load run %q: %w", runID, err)
	}
	events, err := st.LoadRunEvents(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("load run events %q: %w", runID, err)
	}

	checksum, err := computeChecksum(rec, events)
	if err != nil {
		return nil, fmt.Errorf("compute bundle checksum: %w", err)
	}

	return &FlightBundle{
		FormatVersion:  FormatVersionV1,
		ExportedAtNs:   time.Now().UnixNano(),
		Run:            rec,
		Events:         events,
		ChecksumSHA256: checksum,
	}, nil
}

// VerifyBundle validates a FlightBundle's format version, SHA-256 checksum,
// embedded workflow DAG, and deterministic replay invariants.
func VerifyBundle(b *FlightBundle) error {
	if b == nil {
		return ErrNilBundle
	}
	if b.FormatVersion != FormatVersionV1 {
		return fmt.Errorf("%w: got %q, want %q", ErrInvalidFormatVersion, b.FormatVersion, FormatVersionV1)
	}

	expected, err := computeChecksum(b.Run, b.Events)
	if err != nil {
		return fmt.Errorf("compute bundle checksum: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(b.ChecksumSHA256), []byte(expected)) != 1 {
		return fmt.Errorf("%w: got %q, expected %q", ErrChecksumMismatch, b.ChecksumSHA256, expected)
	}

	dag, err := engine.ParseDAG([]byte(b.Run.WorkflowYAML))
	if err != nil {
		return fmt.Errorf("parse bundle workflow DAG: %w", err)
	}
	if b.Run.WorkflowHash != "" && b.Run.WorkflowHash != dag.WorkflowHash {
		return fmt.Errorf("workflow hash mismatch: bundle=%s parsed=%s", b.Run.WorkflowHash, dag.WorkflowHash)
	}

	if _, err := engine.VerifyReplay(dag, b.Events); err != nil {
		return fmt.Errorf("verify bundle replay: %w", err)
	}
	return nil
}

// ImportBundle verifies a FlightBundle and persists it as a self-contained run
// in st. ParentRunID and ForkFromSeq are cleared because the bundle already
// contains the full resolved event stream (1..N). If overrideRunID is non-empty,
// the run record and all event RunIDs are rewritten to overrideRunID.
func ImportBundle(ctx context.Context, st store.JournalStore, b *FlightBundle, overrideRunID string) (string, error) {
	if st == nil {
		return "", fmt.Errorf("journal store is required")
	}
	if err := VerifyBundle(b); err != nil {
		return "", err
	}

	targetRunID := b.Run.RunID
	if overrideRunID != "" {
		targetRunID = overrideRunID
	}
	if targetRunID == "" {
		return "", fmt.Errorf("target run_id is empty")
	}

	rec := b.Run
	rec.RunID = targetRunID
	rec.ParentRunID = ""
	rec.ForkFromSeq = 0

	events := make([]engine.Event, len(b.Events))
	for i, ev := range b.Events {
		ev.RunID = targetRunID
		events[i] = ev
	}

	if err := st.CreateRun(ctx, rec, events); err != nil {
		return "", fmt.Errorf("create imported run %q: %w", targetRunID, err)
	}
	return targetRunID, nil
}

func computeChecksum(run store.RunRecord, events []engine.Event) (string, error) {
	normalizedEvents := events
	if normalizedEvents == nil {
		normalizedEvents = []engine.Event{}
	}
	raw, err := json.Marshal(canonicalBundlePayload{
		Run:    run,
		Events: normalizedEvents,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
