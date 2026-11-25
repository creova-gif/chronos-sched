package schedule

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const maxCatchUpSlotsPerTick = 1000

// SubmitFunc submits a triggered workflow run with a deterministic runID.
type SubmitFunc func(ctx context.Context, runID string, workflowYAML string) error

// Runner polls a SQLiteStore for due schedules, atomically advances each
// schedule's cursor via CAS, and triggers runs with deterministic idempotent IDs.
type Runner struct {
	store    *SQLiteStore
	submitFn SubmitFunc
	nowFn    func() time.Time
}

// NewRunner constructs a Runner. If nowFn is nil, time.Now().UTC() is used.
func NewRunner(store *SQLiteStore, submitFn SubmitFunc, nowFn func() time.Time) *Runner {
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	return &Runner{
		store:    store,
		submitFn: submitFn,
		nowFn:    nowFn,
	}
}

// DeterministicRunID derives an idempotent run ID for a schedule and its
// scheduled slot timestamp.
func DeterministicRunID(scheduleID string, slotTime time.Time) string {
	return fmt.Sprintf("sched_%s_%d", scheduleID, slotTime.UTC().Unix())
}

// Tick evaluates all enabled due schedules at the current clock reading,
// atomically claims each due slot via Advance, and invokes submitFn.
// It returns the number of runs successfully submitted during this tick.
func (r *Runner) Tick(ctx context.Context) (int, error) {
	if r.store == nil {
		return 0, errors.New("schedule runner store is nil")
	}
	if r.submitFn == nil {
		return 0, errors.New("schedule runner submitFn is nil")
	}

	now := r.nowFn().UTC()
	nowNs := now.UnixNano()

	due, err := r.store.ListDue(ctx, nowNs)
	if err != nil {
		return 0, err
	}

	fired := 0
	var errs []error

	for _, rec := range due {
		n, err := r.fireSchedule(ctx, rec, now)
		fired += n
		if err != nil {
			errs = append(errs, err)
		}
	}

	return fired, errors.Join(errs...)
}

func (r *Runner) fireSchedule(ctx context.Context, rec ScheduleRecord, now time.Time) (int, error) {
	spec, err := ParseSpec(rec.CronExpr)
	if err != nil {
		return 0, fmt.Errorf("parse schedule %q spec %q: %w", rec.ID, rec.CronExpr, err)
	}

	nowNs := now.UnixNano()
	policy := rec.MisfirePolicy
	if policy == "" {
		policy = MisfireSkip
	}

	slotNs := rec.NextRunAtNs
	fired := 0

	maxIterations := 1
	if policy == MisfireCatchUp {
		maxIterations = maxCatchUpSlotsPerTick
	}

	for i := 0; i < maxIterations && slotNs <= nowNs; i++ {
		slotTime := time.Unix(0, slotNs).UTC()

		var nextTime time.Time
		if policy == MisfireCatchUp {
			nextTime = spec.Next(slotTime)
		} else {
			nextTime = spec.Next(now)
		}
		if nextTime.IsZero() {
			return fired, fmt.Errorf("schedule %q spec %q produced no next run time", rec.ID, rec.CronExpr)
		}
		newNextNs := nextTime.UnixNano()

		claimed, err := r.store.Advance(ctx, rec.ID, slotNs, slotNs, newNextNs)
		if err != nil {
			return fired, fmt.Errorf("advance schedule %q: %w", rec.ID, err)
		}
		if !claimed {
			// Another tick or coordinator instance already claimed this slot.
			return fired, nil
		}

		runID := DeterministicRunID(rec.ID, slotTime)
		if err := r.submitFn(ctx, runID, rec.WorkflowYAML); err != nil {
			return fired, fmt.Errorf("submit schedule %q run %q: %w", rec.ID, runID, err)
		}
		fired++
		slotNs = newNextNs
	}

	return fired, nil
}

// Run ticks on interval until ctx is canceled.
func (r *Runner) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("runner poll interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.Tick(ctx); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}
