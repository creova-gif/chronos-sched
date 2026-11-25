package ha

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLeaseLost is returned when a leader attempts to renew a lease that
	// has expired, been released, or been superseded by a higher epoch.
	ErrLeaseLost = errors.New("ha: leader lease lost")

	// ErrNoLeader is returned by CurrentLeader when no active, unexpired
	// leader holds the lock.
	ErrNoLeader = errors.New("ha: no active leader")
)

const defaultReleaseTimeout = time.Second

// LeaderLease represents a time-bounded leadership lease fenced by a
// monotonically increasing Epoch.
type LeaderLease struct {
	LockName    string
	HolderID    string
	Epoch       int64
	ExpiresAtNs int64
}

// LeaderElector coordinates active-standby coordinator leader election.
type LeaderElector interface {
	TryAcquire(ctx context.Context, candidateID string, ttl time.Duration) (LeaderLease, bool, error)
	Renew(ctx context.Context, lease LeaderLease, ttl time.Duration) (LeaderLease, error)
	Release(ctx context.Context, lease LeaderLease) error
	CurrentLeader(ctx context.Context) (LeaderLease, error)
}

// RunLeaderLoop runs the active-standby coordinator election loop until ctx is
// cancelled.
//
// While standby, it attempts TryAcquire at renewInterval cadence. Upon
// election, it creates a child leaderCtx, invokes onElected in a goroutine, and
// renews the lease every renewInterval. If Renew fails or returns ErrLeaseLost,
// it immediately cancels leaderCtx, waits for onElected to return, invokes
// onLost, and returns to standby. On parent ctx cancellation, it cancels
// leaderCtx, waits for onElected to return, and best-effort releases the lease
// so a standby can take over immediately.
func RunLeaderLoop(
	ctx context.Context,
	elector LeaderElector,
	candidateID string,
	ttl time.Duration,
	renewInterval time.Duration,
	onElected func(leaderCtx context.Context, lease LeaderLease),
	onLost func(),
) {
	if renewInterval <= 0 {
		renewInterval = ttl / 3
		if renewInterval <= 0 {
			renewInterval = 100 * time.Millisecond
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		lease, acquired, err := elector.TryAcquire(ctx, candidateID, ttl)
		if err == nil && acquired {
			runAsLeader(ctx, elector, lease, ttl, renewInterval, onElected, onLost)
			if ctx.Err() != nil {
				return
			}
		}

		timer := time.NewTimer(renewInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func runAsLeader(
	ctx context.Context,
	elector LeaderElector,
	lease LeaderLease,
	ttl time.Duration,
	renewInterval time.Duration,
	onElected func(leaderCtx context.Context, lease LeaderLease),
	onLost func(),
) {
	leaderCtx, cancelLeader := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if onElected != nil {
			onElected(leaderCtx, lease)
		}
	}()

	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	bestEffortRelease := func(current LeaderLease) {
		relCtx, relCancel := context.WithTimeout(context.Background(), defaultReleaseTimeout)
		defer relCancel()
		_ = elector.Release(relCtx, current)
	}

	for {
		select {
		case <-ctx.Done():
			cancelLeader()
			<-done
			bestEffortRelease(lease)
			return

		case <-done:
			cancelLeader()
			bestEffortRelease(lease)
			if ctx.Err() == nil && onLost != nil {
				onLost()
			}
			return

		case <-ticker.C:
			renewed, err := elector.Renew(ctx, lease, ttl)
			if err != nil {
				cancelLeader()
				<-done
				if ctx.Err() != nil {
					bestEffortRelease(lease)
					return
				}
				if onLost != nil {
					onLost()
				}
				return
			}
			lease = renewed
		}
	}
}
