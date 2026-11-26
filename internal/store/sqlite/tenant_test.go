package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/creova-gif/chronos-sched/internal/engine"
	"github.com/creova-gif/chronos-sched/internal/store"
	"github.com/creova-gif/chronos-sched/internal/store/sqlite"
)

func createTenantTestRun(t *testing.T, ctx context.Context, st *sqlite.Store, runID string, status engine.RunStatus, createdAtNs int64) {
	t.Helper()
	err := st.CreateRun(ctx, store.RunRecord{
		RunID:        runID,
		WorkflowName: "wf-tenant",
		WorkflowHash: "hash-1",
		WorkflowYAML: "name: wf-tenant\n",
		Status:       status,
		CreatedAtNs:  createdAtNs,
	}, nil)
	if err != nil {
		t.Fatalf("CreateRun(%s): %v", runID, err)
	}
}

func TestTenantStoreSetGetAndList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-tenant.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	baseTime := time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC).UnixNano()
	createTenantTestRun(t, ctx, st, "run-default-implicit", engine.RunStatusRunning, baseTime)
	createTenantTestRun(t, ctx, st, "run-default-empty", engine.RunStatusCompleted, baseTime+int64(time.Second))
	createTenantTestRun(t, ctx, st, "run-acme-1", engine.RunStatusRunning, baseTime+2*int64(time.Second))
	createTenantTestRun(t, ctx, st, "run-acme-2", engine.RunStatusCompleted, baseTime+3*int64(time.Second))
	createTenantTestRun(t, ctx, st, "run-globex-1", engine.RunStatusFailed, baseTime+4*int64(time.Second))

	// Unassigned run defaults to DefaultTenant.
	got, err := st.GetRunTenant(ctx, "run-default-implicit")
	if err != nil {
		t.Fatalf("GetRunTenant(implicit): %v", err)
	}
	if got != store.DefaultTenant {
		t.Fatalf("expected %q, got %q", store.DefaultTenant, got)
	}

	// Empty tenantID defaults to DefaultTenant.
	if err := st.SetRunTenant(ctx, "run-default-empty", ""); err != nil {
		t.Fatalf("SetRunTenant(empty): %v", err)
	}
	got, err = st.GetRunTenant(ctx, "run-default-empty")
	if err != nil {
		t.Fatalf("GetRunTenant(empty): %v", err)
	}
	if got != store.DefaultTenant {
		t.Fatalf("expected %q, got %q", store.DefaultTenant, got)
	}

	// Assign explicit tenants.
	if err := st.SetRunTenant(ctx, "run-acme-1", "acme"); err != nil {
		t.Fatalf("SetRunTenant(run-acme-1): %v", err)
	}
	if err := st.SetRunTenant(ctx, "run-acme-2", "acme"); err != nil {
		t.Fatalf("SetRunTenant(run-acme-2): %v", err)
	}
	if err := st.SetRunTenant(ctx, "run-globex-1", "globex"); err != nil {
		t.Fatalf("SetRunTenant(run-globex-1): %v", err)
	}

	got, err = st.GetRunTenant(ctx, "run-acme-1")
	if err != nil {
		t.Fatalf("GetRunTenant(run-acme-1): %v", err)
	}
	if got != "acme" {
		t.Fatalf("expected %q, got %q", "acme", got)
	}

	// Verify listing ordered newest-first.
	acmeAll, err := st.ListRunIDsByTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(acme): %v", err)
	}
	wantAcmeAll := []string{"run-acme-2", "run-acme-1"}
	if !reflect.DeepEqual(acmeAll, wantAcmeAll) {
		t.Fatalf("ListRunIDsByTenant(acme) = %v, want %v", acmeAll, wantAcmeAll)
	}

	// Verify status filtering.
	acmeRunning, err := st.ListRunIDsByTenant(ctx, "acme", engine.RunStatusRunning)
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(acme, RUNNING): %v", err)
	}
	wantAcmeRunning := []string{"run-acme-1"}
	if !reflect.DeepEqual(acmeRunning, wantAcmeRunning) {
		t.Fatalf("ListRunIDsByTenant(acme, RUNNING) = %v, want %v", acmeRunning, wantAcmeRunning)
	}

	// Default tenant includes both implicit and explicitly defaulted runs.
	defaultAll, err := st.ListRunIDsByTenant(ctx, store.DefaultTenant)
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(default): %v", err)
	}
	wantDefaultAll := []string{"run-default-empty", "run-default-implicit"}
	if !reflect.DeepEqual(defaultAll, wantDefaultAll) {
		t.Fatalf("ListRunIDsByTenant(default) = %v, want %v", defaultAll, wantDefaultAll)
	}

	// Upsert reassigns tenant cleanly.
	if err := st.SetRunTenant(ctx, "run-globex-1", "acme"); err != nil {
		t.Fatalf("SetRunTenant upsert: %v", err)
	}
	globexAfter, err := st.ListRunIDsByTenant(ctx, "globex")
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(globex): %v", err)
	}
	if len(globexAfter) != 0 {
		t.Fatalf("expected 0 globex runs after reassignment, got %v", globexAfter)
	}
	acmeAfter, err := st.ListRunIDsByTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(acme) after upsert: %v", err)
	}
	wantAcmeAfter := []string{"run-globex-1", "run-acme-2", "run-acme-1"}
	if !reflect.DeepEqual(acmeAfter, wantAcmeAfter) {
		t.Fatalf("ListRunIDsByTenant(acme) after upsert = %v, want %v", acmeAfter, wantAcmeAfter)
	}
}

func TestTenantStoreReadOnlyBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-tenant-ro.db")

	rw, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	baseTime := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC).UnixNano()
	createTenantTestRun(t, ctx, rw, "run-1", engine.RunStatusRunning, baseTime)
	createTenantTestRun(t, ctx, rw, "run-2", engine.RunStatusCompleted, baseTime+int64(time.Second))
	if err := rw.Close(); err != nil {
		t.Fatalf("Close rw: %v", err)
	}

	roBefore, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly before tenant writes: %v", err)
	}

	// Missing run_tenants table returns DefaultTenant on GetRunTenant.
	tenant, err := roBefore.GetRunTenant(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRunTenant on missing table: %v", err)
	}
	if tenant != store.DefaultTenant {
		t.Fatalf("expected %q on missing table, got %q", store.DefaultTenant, tenant)
	}

	// Missing run_tenants table falls back to runs for DefaultTenant.
	defaultRuns, err := roBefore.ListRunIDsByTenant(ctx, store.DefaultTenant)
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(default) on missing table: %v", err)
	}
	if !reflect.DeepEqual(defaultRuns, []string{"run-2", "run-1"}) {
		t.Fatalf("expected [run-2 run-1], got %v", defaultRuns)
	}

	// Fallback also respects status filters.
	defaultRunning, err := roBefore.ListRunIDsByTenant(ctx, store.DefaultTenant, engine.RunStatusRunning)
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(default, RUNNING) on missing table: %v", err)
	}
	if !reflect.DeepEqual(defaultRunning, []string{"run-1"}) {
		t.Fatalf("expected [run-1], got %v", defaultRunning)
	}

	// Non-default tenant returns empty slice on missing table.
	acmeRuns, err := roBefore.ListRunIDsByTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(acme) on missing table: %v", err)
	}
	if len(acmeRuns) != 0 {
		t.Fatalf("expected 0 runs for acme on missing table, got %v", acmeRuns)
	}

	// Mutating calls must return store.ErrStoreReadOnly.
	if err := roBefore.SetRunTenant(ctx, "run-1", "acme"); !errors.Is(err, store.ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly from SetRunTenant, got %v", err)
	}
	if err := roBefore.Close(); err != nil {
		t.Fatalf("Close roBefore: %v", err)
	}

	// Re-open read-write, assign a tenant, and verify OpenReadOnly sees it.
	rw2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open rw2: %v", err)
	}
	if err := rw2.SetRunTenant(ctx, "run-1", "acme"); err != nil {
		t.Fatalf("SetRunTenant: %v", err)
	}
	if err := rw2.Close(); err != nil {
		t.Fatalf("Close rw2: %v", err)
	}

	roAfter, err := sqlite.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnly after tenant writes: %v", err)
	}
	defer func() { _ = roAfter.Close() }()

	gotAcme, err := roAfter.GetRunTenant(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRunTenant(run-1): %v", err)
	}
	if gotAcme != "acme" {
		t.Fatalf("expected %q, got %q", "acme", gotAcme)
	}

	acmeAfter, err := roAfter.ListRunIDsByTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(acme): %v", err)
	}
	if !reflect.DeepEqual(acmeAfter, []string{"run-1"}) {
		t.Fatalf("expected [run-1], got %v", acmeAfter)
	}

	defaultAfter, err := roAfter.ListRunIDsByTenant(ctx, store.DefaultTenant)
	if err != nil {
		t.Fatalf("ListRunIDsByTenant(default): %v", err)
	}
	if !reflect.DeepEqual(defaultAfter, []string{"run-2"}) {
		t.Fatalf("expected [run-2], got %v", defaultAfter)
	}
}

func TestTenantStoreConcurrentAccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "chronos-tenant-race.db")
	st, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	const workers = 8
	baseTime := time.Date(2026, 11, 26, 11, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i < workers; i++ {
		createTenantTestRun(t, ctx, st, fmt.Sprintf("run-%d", i), engine.RunStatusRunning, baseTime+int64(i))
	}

	var wg sync.WaitGroup
	wg.Add(workers * 3)

	for i := 0; i < workers; i++ {
		i := i
		runID := fmt.Sprintf("run-%d", i)
		tenantID := fmt.Sprintf("tenant-%d", i%2)

		go func() {
			defer wg.Done()
			if err := st.SetRunTenant(ctx, runID, tenantID); err != nil {
				t.Errorf("SetRunTenant(%s, %s): %v", runID, tenantID, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := st.GetRunTenant(ctx, runID); err != nil {
				t.Errorf("GetRunTenant(%s): %v", runID, err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := st.ListRunIDsByTenant(ctx, tenantID, engine.RunStatusRunning); err != nil {
				t.Errorf("ListRunIDsByTenant(%s): %v", tenantID, err)
			}
		}()
	}
	wg.Wait()

	t0, err := st.ListRunIDsByTenant(ctx, "tenant-0")
	if err != nil {
		t.Fatalf("final ListRunIDsByTenant(tenant-0): %v", err)
	}
	t1, err := st.ListRunIDsByTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("final ListRunIDsByTenant(tenant-1): %v", err)
	}
	if len(t0) != workers/2 || len(t1) != workers/2 {
		t.Fatalf("expected %d runs per tenant, got tenant-0=%d tenant-1=%d", workers/2, len(t0), len(t1))
	}
}
