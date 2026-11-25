package ha

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func openTestElector(t *testing.T, dbPath string, clock *fakeClock) *SQLiteElector {
	t.Helper()
	el, err := OpenSQLiteElector(dbPath, "test-coordinator")
	if err != nil {
		t.Fatalf("OpenSQLiteElector: %v", err)
	}
	if clock != nil {
		el.SetNowFunc(clock.Now)
	}
	t.Cleanup(func() {
		if err := el.Close(); err != nil {
			t.Errorf("close elector: %v", err)
		}
	})
	return el
}

func TestSQLiteElector_MutualExclusion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ha.db")
	clock := newFakeClock(time.Unix(1_700_000_000, 0))

	e1 := openTestElector(t, dbPath, clock)
	e2 := openTestElector(t, dbPath, clock)

	type result struct {
		id       string
		lease    LeaderLease
		acquired bool
		err      error
	}

	ctx := context.Background()
	start := make(chan struct{})
	resCh := make(chan result, 2)

	tryCandidate := func(el *SQLiteElector, id string) {
		<-start
		lease, ok, err := el.TryAcquire(ctx, id, 5*time.Second)
		resCh <- result{id: id, lease: lease, acquired: ok, err: err}
	}

	go tryCandidate(e1, "coord-1")
	go tryCandidate(e2, "coord-2")
	close(start)

	var winners []result
	var losers []result
	for range 2 {
		r := <-resCh
		if r.err != nil {
			t.Fatalf("TryAcquire(%s) unexpected error: %v", r.id, r.err)
		}
		if r.acquired {
			winners = append(winners, r)
		} else {
			losers = append(losers, r)
		}
	}

	if len(winners) != 1 || len(losers) != 1 {
		t.Fatalf("expected exactly 1 winner and 1 loser, got winners=%d losers=%d", len(winners), len(losers))
	}
	if winners[0].lease.Epoch != 1 {
		t.Fatalf("expected initial epoch=1, got %d", winners[0].lease.Epoch)
	}
	if losers[0].lease.HolderID != winners[0].id {
		t.Fatalf("expected loser to observe winner %q, got %q", winners[0].id, losers[0].lease.HolderID)
	}

	cur, err := e1.CurrentLeader(ctx)
	if err != nil {
		t.Fatalf("CurrentLeader: %v", err)
	}
	if cur.HolderID != winners[0].id || cur.Epoch != 1 {
		t.Fatalf("unexpected CurrentLeader: %+v", cur)
	}
}

func TestSQLiteElector_MonotonicEpochAndFencing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ha.db")
	clock := newFakeClock(time.Unix(1_700_000_000, 0))

	e1 := openTestElector(t, dbPath, clock)
	e2 := openTestElector(t, dbPath, clock)
	ctx := context.Background()

	if _, err := e1.CurrentLeader(ctx); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader initially, got %v", err)
	}

	// Leader 1 acquires epoch 1.
	lease1, ok, err := e1.TryAcquire(ctx, "coord-1", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("leader 1 acquire failed: ok=%v err=%v", ok, err)
	}
	if lease1.Epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", lease1.Epoch)
	}

	// Leader 1 renews cleanly before expiry.
	clock.Advance(1 * time.Second)
	lease1, err = e1.Renew(ctx, lease1, 2*time.Second)
	if err != nil {
		t.Fatalf("leader 1 renew failed: %v", err)
	}
	if lease1.Epoch != 1 {
		t.Fatalf("expected epoch to remain 1 on renew, got %d", lease1.Epoch)
	}

	// Expire leader 1's lease.
	clock.Advance(3 * time.Second)
	if _, err := e2.CurrentLeader(ctx); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader after expiry, got %v", err)
	}

	// Leader 2 takes over with monotonic epoch == 2.
	lease2, ok, err := e2.TryAcquire(ctx, "coord-2", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("leader 2 takeover failed: ok=%v err=%v", ok, err)
	}
	if lease2.Epoch != 2 {
		t.Fatalf("expected epoch 2 on takeover, got %d", lease2.Epoch)
	}

	// Leader 1's stale Renew MUST fail with ErrLeaseLost.
	if _, err := e1.Renew(ctx, lease1, 2*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost for stale renew, got %v", err)
	}

	// Leader 1's stale Release MUST NOT evict Leader 2.
	if err := e1.Release(ctx, lease1); err != nil {
		t.Fatalf("stale release returned error: %v", err)
	}
	cur, err := e2.CurrentLeader(ctx)
	if err != nil {
		t.Fatalf("CurrentLeader after stale release: %v", err)
	}
	if cur.HolderID != "coord-2" || cur.Epoch != 2 {
		t.Fatalf("stale release corrupted current leader: %+v", cur)
	}

	// Graceful release by Leader 2 followed by re-acquire by Leader 1 MUST still
	// advance epoch monotonically to 3.
	if err := e2.Release(ctx, lease2); err != nil {
		t.Fatalf("leader 2 release failed: %v", err)
	}
	if _, err := e1.CurrentLeader(ctx); !errors.Is(err, ErrNoLeader) {
		t.Fatalf("expected ErrNoLeader after release, got %v", err)
	}

	lease3, ok, err := e1.TryAcquire(ctx, "coord-1", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("leader 1 re-acquire failed: ok=%v err=%v", ok, err)
	}
	if lease3.Epoch != 3 {
		t.Fatalf("expected monotonic epoch 3 after release, got %d", lease3.Epoch)
	}
}

func TestRunLeaderLoop_ActiveStandbyFailover(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ha.db")
	e1 := openTestElector(t, dbPath, nil)
	e2 := openTestElector(t, dbPath, nil)

	const (
		ttl           = 300 * time.Millisecond
		renewInterval = 40 * time.Millisecond
	)

	var activeCount atomic.Int32
	var overlapDetected atomic.Bool

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	elected1Ch := make(chan LeaderLease, 1)
	elected2Ch := make(chan LeaderLease, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		RunLeaderLoop(ctx1, e1, "coord-1", ttl, renewInterval,
			func(leaderCtx context.Context, lease LeaderLease) {
				if activeCount.Add(1) > 1 {
					overlapDetected.Store(true)
				}
				defer activeCount.Add(-1)
				select {
				case elected1Ch <- lease:
				default:
				}
				<-leaderCtx.Done()
			},
			nil,
		)
	}()

	var lease1 LeaderLease
	select {
	case lease1 = <-elected1Ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for coord-1 to become leader")
	}
	if lease1.HolderID != "coord-1" || lease1.Epoch != 1 {
		t.Fatalf("unexpected lease1: %+v", lease1)
	}

	// Start standby coordinator 2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		RunLeaderLoop(ctx2, e2, "coord-2", ttl, renewInterval,
			func(leaderCtx context.Context, lease LeaderLease) {
				if activeCount.Add(1) > 1 {
					overlapDetected.Store(true)
				}
				defer activeCount.Add(-1)
				select {
				case elected2Ch <- lease:
				default:
				}
				<-leaderCtx.Done()
			},
			nil,
		)
	}()

	// Verify coord-2 remains standby while coord-1 is healthy and renewing.
	select {
	case unexpected := <-elected2Ch:
		t.Fatalf("coord-2 became leader while coord-1 was active: %+v", unexpected)
	case <-time.After(150 * time.Millisecond):
	}

	// Gracefully stop leader 1; standby coord-2 must take over with a higher epoch.
	cancel1()

	var lease2 LeaderLease
	select {
	case lease2 = <-elected2Ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for coord-2 failover takeover")
	}
	if lease2.HolderID != "coord-2" || lease2.Epoch <= lease1.Epoch {
		t.Fatalf("expected coord-2 with epoch > %d, got %+v", lease1.Epoch, lease2)
	}

	cancel2()
	wg.Wait()

	if overlapDetected.Load() {
		t.Fatal("detected split-brain overlap between active leaders")
	}
}

func TestRunLeaderLoop_LeaseLossTriggersOnLost(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ha.db")
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	e1 := openTestElector(t, dbPath, clock)
	e2 := openTestElector(t, dbPath, clock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	electedCh := make(chan LeaderLease, 2)
	onElectedExited := make(chan struct{}, 2)
	onLostCh := make(chan struct{}, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		RunLeaderLoop(ctx, e1, "coord-1", 500*time.Millisecond, 30*time.Millisecond,
			func(leaderCtx context.Context, lease LeaderLease) {
				select {
				case electedCh <- lease:
				default:
				}
				<-leaderCtx.Done()
				onElectedExited <- struct{}{}
			},
			func() {
				onLostCh <- struct{}{}
			},
		)
	}()

	select {
	case <-electedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial election")
	}

	// Simulate clock jump / partition causing coord-1's lease to expire and
	// coord-2 to steal leadership before coord-1's next Renew.
	clock.Advance(2 * time.Second)
	if _, ok, err := e2.TryAcquire(ctx, "coord-2", 5*time.Second); err != nil || !ok {
		t.Fatalf("coord-2 takeover failed: ok=%v err=%v", ok, err)
	}

	select {
	case <-onElectedExited:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for leaderCtx cancellation after lease loss")
	}

	select {
	case <-onLostCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onLost callback after lease loss")
	}

	cancel()
	wg.Wait()
}
