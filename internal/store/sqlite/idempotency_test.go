package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func TestIdempotencyStorePutGetTenantIsolationAndReadOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-idempotency.db")

	// Initialize the SQLite database file without creating idempotency_keys yet.
	initStore, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open(init): %v", err)
	}
	if err := initStore.Close(); err != nil {
		t.Fatalf("Close(init): %v", err)
	}

	// OpenReadOnly on an existing DB before idempotency_keys exists:
	// reads must gracefully return (zero, false, nil) and writes must fail with ErrStoreReadOnly.
	roBefore, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly(before): %v", err)
	}
	if got, ok, err := roBefore.GetIdempotencyKey(ctx, "acme", "k-missing"); err != nil || ok || got != (store.IdempotencyRecord{}) {
		t.Fatalf("read-only missing table: got (%+v, %v, %v), want (zero, false, nil)", got, ok, err)
	}
	if err := roBefore.PutIdempotencyKey(ctx, store.IdempotencyRecord{
		TenantID:     "acme",
		Key:          "k-ro",
		RunID:        "run-ro",
		WorkflowHash: "hash-ro",
		CreatedAtNs:  1,
	}); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly, got %v", err)
	}
	_ = roBefore.Close()

	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	baseTime := time.Date(2026, 11, 26, 11, 0, 0, 0, time.UTC).UnixNano()

	// Empty TenantID defaults to store.DefaultTenant.
	defaultRec := store.IdempotencyRecord{
		Key:          "req-1",
		RunID:        "run-default-1",
		WorkflowHash: "hash-default-1",
		CreatedAtNs:  baseTime,
	}
	if err := st.PutIdempotencyKey(ctx, defaultRec); err != nil {
		t.Fatalf("PutIdempotencyKey(default): %v", err)
	}
	got, ok, err := st.GetIdempotencyKey(ctx, "", "req-1")
	if err != nil || !ok {
		t.Fatalf("GetIdempotencyKey(empty tenant): ok=%v err=%v", ok, err)
	}
	wantDefault := defaultRec
	wantDefault.TenantID = store.DefaultTenant
	if got != wantDefault {
		t.Fatalf("got %+v, want %+v", got, wantDefault)
	}

	// Duplicate put for the same (tenant, key) must DO NOTHING and preserve original record.
	if err := st.PutIdempotencyKey(ctx, store.IdempotencyRecord{
		TenantID:     store.DefaultTenant,
		Key:          "req-1",
		RunID:        "run-default-2",
		WorkflowHash: "hash-default-2",
		CreatedAtNs:  baseTime + 100,
	}); err != nil {
		t.Fatalf("PutIdempotencyKey(duplicate): %v", err)
	}
	got, ok, err = st.GetIdempotencyKey(ctx, store.DefaultTenant, "req-1")
	if err != nil || !ok || got != wantDefault {
		t.Fatalf("duplicate put overwrote record: got (%+v, %v, %v), want (%+v, true, nil)", got, ok, err, wantDefault)
	}

	// Tenant isolation: same key under different tenants is independent.
	acmeRec := store.IdempotencyRecord{
		TenantID:     "acme",
		Key:          "shared-key",
		RunID:        "run-acme-1",
		WorkflowHash: "hash-acme",
		CreatedAtNs:  baseTime + 10,
	}
	globexRec := store.IdempotencyRecord{
		TenantID:     "globex",
		Key:          "shared-key",
		RunID:        "run-globex-1",
		WorkflowHash: "hash-globex",
		CreatedAtNs:  baseTime + 20,
	}
	if err := st.PutIdempotencyKey(ctx, acmeRec); err != nil {
		t.Fatalf("PutIdempotencyKey(acme): %v", err)
	}
	if err := st.PutIdempotencyKey(ctx, globexRec); err != nil {
		t.Fatalf("PutIdempotencyKey(globex): %v", err)
	}

	gotAcme, ok, err := st.GetIdempotencyKey(ctx, "acme", "shared-key")
	if err != nil || !ok || gotAcme != acmeRec {
		t.Fatalf("acme record mismatch: got (%+v, %v, %v), want %+v", gotAcme, ok, err, acmeRec)
	}
	gotGlobex, ok, err := st.GetIdempotencyKey(ctx, "globex", "shared-key")
	if err != nil || !ok || gotGlobex != globexRec {
		t.Fatalf("globex record mismatch: got (%+v, %v, %v), want %+v", gotGlobex, ok, err, globexRec)
	}
	if gotOther, ok, err := st.GetIdempotencyKey(ctx, "initech", "shared-key"); err != nil || ok || gotOther != (store.IdempotencyRecord{}) {
		t.Fatalf("cross-tenant leak: got (%+v, %v, %v), want (zero, false, nil)", gotOther, ok, err)
	}

	// OpenReadOnly after writes can read existing keys and still rejects writes.
	roAfter, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly(after): %v", err)
	}
	defer func() { _ = roAfter.Close() }()

	gotRO, ok, err := roAfter.GetIdempotencyKey(ctx, "acme", "shared-key")
	if err != nil || !ok || gotRO != acmeRec {
		t.Fatalf("read-only get after write: got (%+v, %v, %v), want %+v", gotRO, ok, err, acmeRec)
	}
	if err := roAfter.PutIdempotencyKey(ctx, acmeRec); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("read-only put after write: expected ErrStoreReadOnly, got %v", err)
	}

	// Concurrent put/get access under -race.
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers*2)
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tenant := fmt.Sprintf("tenant-%d", idx%4)
			key := fmt.Sprintf("concurrent-%d", idx)
			rec := store.IdempotencyRecord{
				TenantID:     tenant,
				Key:          key,
				RunID:        fmt.Sprintf("run-%d", idx),
				WorkflowHash: fmt.Sprintf("hash-%d", idx),
				CreatedAtNs:  baseTime + int64(idx),
			}
			if err := st.PutIdempotencyKey(ctx, rec); err != nil {
				errs <- fmt.Errorf("put %d: %w", idx, err)
				return
			}
			got, ok, err := st.GetIdempotencyKey(ctx, tenant, key)
			if err != nil {
				errs <- fmt.Errorf("get %d: %w", idx, err)
				return
			}
			if !ok || got != rec {
				errs <- fmt.Errorf("get %d mismatch: got (%+v, %v), want %+v", idx, got, ok, rec)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestIdempotencyStorePrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-idempotency-prune.db")

	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Pruning before idempotency_keys exists returns (0, nil).
	deleted, err := st.PruneIdempotencyKeys(ctx, 1000)
	if err != nil || deleted != 0 {
		t.Fatalf("PruneIdempotencyKeys(missing table): got (%d, %v), want (0, nil)", deleted, err)
	}

	cutoffNs := int64(1000)
	records := []store.IdempotencyRecord{
		{TenantID: "acme", Key: "old-1", RunID: "run-old-1", WorkflowHash: "h1", CreatedAtNs: cutoffNs - 500},
		{TenantID: "globex", Key: "old-2", RunID: "run-old-2", WorkflowHash: "h2", CreatedAtNs: cutoffNs - 1},
		{TenantID: "acme", Key: "boundary", RunID: "run-boundary", WorkflowHash: "h3", CreatedAtNs: cutoffNs},
		{TenantID: "acme", Key: "new-1", RunID: "run-new-1", WorkflowHash: "h4", CreatedAtNs: cutoffNs + 500},
	}
	for _, rec := range records {
		if err := st.PutIdempotencyKey(ctx, rec); err != nil {
			t.Fatalf("PutIdempotencyKey(%s): %v", rec.Key, err)
		}
	}

	// OpenReadOnly rejects PruneIdempotencyKeys with ErrStoreReadOnly.
	ro, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer func() { _ = ro.Close() }()
	if _, err := ro.PruneIdempotencyKeys(ctx, cutoffNs); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("read-only prune: expected ErrStoreReadOnly, got %v", err)
	}

	// Keys strictly older than cutoffNs are deleted; keys >= cutoffNs are retained.
	deleted, err = st.PruneIdempotencyKeys(ctx, cutoffNs)
	if err != nil {
		t.Fatalf("PruneIdempotencyKeys: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("PruneIdempotencyKeys deleted %d rows, want 2", deleted)
	}

	for _, rec := range records[:2] {
		if _, ok, err := st.GetIdempotencyKey(ctx, rec.TenantID, rec.Key); err != nil || ok {
			t.Fatalf("expected old key %q to be deleted, got (ok=%v, err=%v)", rec.Key, ok, err)
		}
	}
	for _, rec := range records[2:] {
		got, ok, err := st.GetIdempotencyKey(ctx, rec.TenantID, rec.Key)
		if err != nil || !ok || got != rec {
			t.Fatalf("expected key %q to be retained, got (%+v, %v, %v)", rec.Key, got, ok, err)
		}
	}
}
