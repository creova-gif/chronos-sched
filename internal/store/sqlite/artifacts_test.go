package sqlite_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestArtifactsPutGetListAndUpsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-artifacts.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	payload1 := []byte("hello, chronos artifact v1")
	sum1 := sha256.Sum256(payload1)
	wantHash1 := hex.EncodeToString(sum1[:])

	put1, err := st.PutArtifact(ctx, store.ArtifactRecord{
		RunID:  "run-1",
		TaskID: "extract",
		Name:   "output.txt",
		Data:   payload1,
	})
	if err != nil {
		t.Fatalf("PutArtifact(v1): %v", err)
	}
	if put1.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
	if put1.SHA256 != wantHash1 {
		t.Fatalf("SHA256 mismatch: got %q, want %q", put1.SHA256, wantHash1)
	}
	if put1.SizeBytes != int64(len(payload1)) {
		t.Fatalf("SizeBytes mismatch: got %d, want %d", put1.SizeBytes, len(payload1))
	}
	if put1.ContentType != "application/octet-stream" {
		t.Fatalf("expected default content type application/octet-stream, got %q", put1.ContentType)
	}
	if put1.CreatedAtNs <= 0 {
		t.Fatalf("expected positive CreatedAtNs, got %d", put1.CreatedAtNs)
	}

	// Second artifact in the same run.
	payload2 := []byte(`{"rows":42}`)
	put2, err := st.PutArtifact(ctx, store.ArtifactRecord{
		RunID:       "run-1",
		TaskID:      "transform",
		Name:        "summary.json",
		ContentType: "application/json",
		Data:        payload2,
		CreatedAtNs: time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC).UnixNano(),
	})
	if err != nil {
		t.Fatalf("PutArtifact(summary.json): %v", err)
	}

	// Upsert the first artifact with new content.
	payload1Updated := []byte("updated artifact payload v2 - longer")
	sum1Updated := sha256.Sum256(payload1Updated)
	wantHash1Updated := hex.EncodeToString(sum1Updated[:])

	upserted, err := st.PutArtifact(ctx, store.ArtifactRecord{
		RunID:       "run-1",
		TaskID:      "extract",
		Name:        "output.txt",
		ContentType: "text/plain",
		Data:        payload1Updated,
	})
	if err != nil {
		t.Fatalf("PutArtifact(upsert): %v", err)
	}
	if upserted.ID != put1.ID {
		t.Fatalf("expected upsert to preserve ID %d, got %d", put1.ID, upserted.ID)
	}
	if upserted.SHA256 != wantHash1Updated {
		t.Fatalf("upserted SHA256 mismatch: got %q, want %q", upserted.SHA256, wantHash1Updated)
	}
	if upserted.SizeBytes != int64(len(payload1Updated)) {
		t.Fatalf("upserted SizeBytes mismatch: got %d, want %d", upserted.SizeBytes, len(payload1Updated))
	}

	// GetArtifact returns full record including Data.
	got, err := st.GetArtifact(ctx, "run-1", "extract", "output.txt")
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if !bytes.Equal(got.Data, payload1Updated) {
		t.Fatalf("GetArtifact Data mismatch: got %q, want %q", got.Data, payload1Updated)
	}
	if got.ContentType != "text/plain" || got.SHA256 != wantHash1Updated {
		t.Fatalf("GetArtifact metadata mismatch: %+v", got)
	}

	// Missing artifact returns sql.ErrNoRows.
	if _, err := st.GetArtifact(ctx, "run-1", "extract", "missing.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for missing artifact, got %v", err)
	}

	// ListRunArtifacts returns metadata ordered by id ASC without Data blobs.
	listed, err := st.ListRunArtifacts(ctx, "run-1")
	if err != nil {
		t.Fatalf("ListRunArtifacts: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 artifacts, got %d", len(listed))
	}
	if listed[0].ID != put1.ID || listed[1].ID != put2.ID {
		t.Fatalf("unexpected ordering: got IDs [%d, %d], want [%d, %d]", listed[0].ID, listed[1].ID, put1.ID, put2.ID)
	}
	for i, item := range listed {
		if len(item.Data) != 0 {
			t.Fatalf("expected ListRunArtifacts[%d].Data to be empty, got %d bytes", i, len(item.Data))
		}
		if item.SizeBytes == 0 || item.SHA256 == "" {
			t.Fatalf("expected ListRunArtifacts[%d] metadata populated, got %+v", i, item)
		}
	}
}

func TestArtifactsReadOnlyBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-artifacts-ro.db")

	// Initialize base database without task_artifacts table.
	rw, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open(rw): %v", err)
	}

	ro, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		_ = rw.Close()
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer func() { _ = ro.Close() }()

	// Before any artifact write, ListRunArtifacts returns empty slice without error.
	listed, err := ro.ListRunArtifacts(ctx, "run-ro")
	if err != nil {
		_ = rw.Close()
		t.Fatalf("ListRunArtifacts(ro before writes): %v", err)
	}
	if len(listed) != 0 {
		_ = rw.Close()
		t.Fatalf("expected 0 artifacts before writes, got %d", len(listed))
	}

	// Before any artifact write, GetArtifact returns sql.ErrNoRows.
	if _, err := ro.GetArtifact(ctx, "run-ro", "t1", "a.bin"); !errors.Is(err, sql.ErrNoRows) {
		_ = rw.Close()
		t.Fatalf("expected sql.ErrNoRows before writes, got %v", err)
	}

	// PutArtifact on read-only store returns ErrStoreReadOnly.
	if _, err := ro.PutArtifact(ctx, store.ArtifactRecord{
		RunID:  "run-ro",
		TaskID: "t1",
		Name:   "a.bin",
		Data:   []byte("nope"),
	}); !errors.Is(err, store.ErrStoreReadOnly) {
		_ = rw.Close()
		t.Fatalf("expected ErrStoreReadOnly, got %v", err)
	}

	// Write via rw store and verify ro sees it.
	if _, err := rw.PutArtifact(ctx, store.ArtifactRecord{
		RunID:  "run-ro",
		TaskID: "t1",
		Name:   "a.bin",
		Data:   []byte("visible"),
	}); err != nil {
		_ = rw.Close()
		t.Fatalf("PutArtifact(rw): %v", err)
	}
	_ = rw.Close()

	listedAfter, err := ro.ListRunArtifacts(ctx, "run-ro")
	if err != nil {
		t.Fatalf("ListRunArtifacts(ro after writes): %v", err)
	}
	if len(listedAfter) != 1 || listedAfter[0].Name != "a.bin" {
		t.Fatalf("expected 1 artifact a.bin, got %+v", listedAfter)
	}
}

func TestArtifactsConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-artifacts-concurrent.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const workers = 8
	const perWorker = 10

	var wg sync.WaitGroup
	errCh := make(chan error, workers*2)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_, err := st.PutArtifact(ctx, store.ArtifactRecord{
					RunID:  "run-concurrent",
					TaskID: fmt.Sprintf("task-%d", workerID),
					Name:   fmt.Sprintf("file-%d.bin", i),
					Data:   []byte(fmt.Sprintf("payload-%d-%d", workerID, i)),
				})
				if err != nil {
					errCh <- fmt.Errorf("worker %d iter %d put: %w", workerID, i, err)
					return
				}
			}
		}(w)

		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := st.ListRunArtifacts(ctx, "run-concurrent"); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d list: %w", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent error: %v", err)
	}

	all, err := st.ListRunArtifacts(ctx, "run-concurrent")
	if err != nil {
		t.Fatalf("ListRunArtifacts(final): %v", err)
	}
	if len(all) != workers*perWorker {
		t.Fatalf("expected %d artifacts, got %d", workers*perWorker, len(all))
	}
}
