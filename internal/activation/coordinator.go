// Package activation coordinates durable post-schedule serving-data actions.
package activation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/safego"
)

var (
	ErrNotNeeded     = errors.New("activation not needed")
	ErrSuperseded    = errors.New("activation source bundle is no longer current")
	ErrUnsupported   = errors.New("activation topology unsupported")
	ErrTargetDeleted = errors.New("activation target deleted")
)

type CapacityError struct {
	Resource   string
	Available  *float64
	Required   *float64
	Units      string
	Reason     string
	RetryAfter time.Duration
}

type RetryableError struct {
	Reason     string
	RetryAfter time.Duration
}

// WaitingError pauses new or reclaimed execution while an earlier generation
// owns resources. Waiting must not exhaust the failed-rollout budget.
type WaitingError struct {
	Reason     string
	RetryAfter time.Duration
}

func (e *WaitingError) Error() string { return e.Reason }

// RepairRequiredError means a rollout crossed its first destructive boundary
// and the fresh surge must remain available until every canonical route is
// healthy again. It is retryable regardless of the ordinary attempt budget.
type RepairRequiredError struct {
	Reason     string
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string      { return e.Reason }
func (e *RepairRequiredError) Error() string { return e.Reason }

func (e *CapacityError) Error() string {
	if e.Reason == "" {
		return "activation deferred for capacity"
	}
	return "activation deferred for capacity: " + e.Reason
}

type Store interface {
	ClaimNextScheduleActivation(now time.Time) (*db.ScheduleActivation, error)
	FinishScheduleActivation(id int64, status, lastError string, finishedAt time.Time, countAttempt bool) error
	DeferScheduleActivation(id int64, status, reason string, dueAt, deferredAt time.Time) error
	WaitScheduleActivation(id int64, reason string, dueAt time.Time) error
}

type Runner interface {
	Roll(ctx context.Context, activation *db.ScheduleActivation) error
}

type Coordinator struct {
	admissionMu  sync.Mutex
	pauses       int
	store        Store
	runner       Runner
	pollInterval time.Duration
	now          func() time.Time
}

func New(store Store, runner Runner, pollInterval time.Duration) *Coordinator {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	return &Coordinator{store: store, runner: runner, pollInterval: pollInterval, now: func() time.Time { return time.Now().UTC() }}
}

func (c *Coordinator) Start(ctx context.Context) {
	for {
		for {
			worked, err := c.ProcessNext(ctx)
			if err != nil {
				slog.Error("schedule activation failed", "error", err)
				// Persistence failures must observe the same poll backoff as an
				// empty queue. Retrying immediately can otherwise hot-loop the only
				// coordinator while the database is unavailable.
				break
			}
			if !worked || ctx.Err() != nil {
				break
			}
		}
		timer := time.NewTimer(c.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Pause prevents new activation claims while an already-admitted roll finishes.
// The caller releases it after a canceled/failed reload; a successful retiring
// process keeps it held until its coordinator stops.
func (c *Coordinator) Pause() func() {
	c.admissionMu.Lock()
	c.pauses++
	c.admissionMu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { c.admissionMu.Lock(); c.pauses--; c.admissionMu.Unlock() }) }
}

func (c *Coordinator) ProcessNext(ctx context.Context) (bool, error) {
	c.admissionMu.Lock()
	now := c.now()
	var a *db.ScheduleActivation
	var err error
	if c.pauses > 0 {
		repairs, ok := c.store.(interface {
			ClaimNextScheduleActivationRepair(time.Time) (*db.ScheduleActivation, error)
		})
		if !ok {
			c.admissionMu.Unlock()
			return false, nil
		}
		a, err = repairs.ClaimNextScheduleActivationRepair(now)
	} else {
		a, err = c.store.ClaimNextScheduleActivation(now)
	}
	c.admissionMu.Unlock()
	if errors.Is(err, db.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim activation: %w", err)
	}
	runID := int64(0)
	if a.ScheduleRunID != nil {
		runID = *a.ScheduleRunID
	}
	// Attempts counts completed rollout executions, not claims. Work with the
	// current execution number while the store records it atomically with the
	// eventual defer/finish transition.
	runActivation := *a
	runActivation.Attempts++
	slog.Info("schedule activation started", "activation_id", a.ID, "schedule_run_id", runID,
		"app", a.AppSlug, "generation", a.TargetGeneration, "attempt", runActivation.Attempts)

	runErr := c.safeRoll(ctx, &runActivation)
	finishedAt := c.now()
	status := "succeeded"
	lastError := ""
	countAttempt := true
	switch {
	case runErr == nil:
	case errors.Is(runErr, ErrNotNeeded):
		// A reclaimed activation whose canonical replicas already carry the
		// target generation completed before its previous terminal DB write.
		// Preserve success attribution (and its damping anchor) on recovery.
		if a.Phase == "recovering" {
			status = "succeeded"
		} else {
			status = "not_needed"
		}
	case errors.Is(runErr, ErrSuperseded):
		status = "superseded"
		countAttempt = false
	case errors.Is(runErr, ErrUnsupported):
		status = "blocked_unsupported"
		lastError = runErr.Error()
	case errors.Is(runErr, ErrTargetDeleted):
		status = "target_deleted"
	case runErr != nil:
		var waiting *WaitingError
		if errors.As(runErr, &waiting) {
			retry := waiting.RetryAfter
			if retry <= 0 {
				retry = 5 * time.Second
			}
			if err := c.store.WaitScheduleActivation(a.ID, waiting.Reason, finishedAt.Add(retry)); err != nil {
				return true, fmt.Errorf("wait activation %d: %w", a.ID, err)
			}
			return true, nil
		}
		var capacity *CapacityError
		if errors.As(runErr, &capacity) {
			capacityDeferredAt := finishedAt
			if a.CapacityDeferredAt != nil {
				capacityDeferredAt = *a.CapacityDeferredAt
			}
			if a.MaxDeferAgeSeconds > 0 {
				expiresAt := capacityDeferredAt.Add(time.Duration(a.MaxDeferAgeSeconds) * time.Second)
				if !finishedAt.Before(expiresAt) {
					status = "failed"
					countAttempt = false
					lastError = fmt.Sprintf("capacity deferral expired after %s: %s",
						time.Duration(a.MaxDeferAgeSeconds)*time.Second, capacity.Reason)
					break
				}
			}
			retry := capacityRetryDelay(capacity.RetryAfter, a.CapacityDeferrals)
			dueAt := finishedAt.Add(retry)
			if a.MaxDeferAgeSeconds > 0 {
				expiresAt := capacityDeferredAt.Add(time.Duration(a.MaxDeferAgeSeconds) * time.Second)
				if dueAt.After(expiresAt) {
					dueAt = expiresAt
				}
			}
			if err := c.store.DeferScheduleActivation(a.ID, "deferred_capacity", capacity.Reason, dueAt, finishedAt); err != nil {
				return true, fmt.Errorf("defer activation %d: %w", a.ID, err)
			}
			level := slog.LevelInfo
			if a.CapacityDeferrals > 0 {
				level = slog.LevelWarn
			}
			slog.Log(ctx, level, "schedule activation capacity blocked", "activation_id", a.ID, "schedule_run_id", runID,
				"app", a.AppSlug, "reason", capacity.Reason, "retry_after", retry,
				"capacity_deferrals", a.CapacityDeferrals+1, "capacity_deferred_at", capacityDeferredAt,
				"deferred_age", finishedAt.Sub(capacityDeferredAt), "max_defer_age_seconds", a.MaxDeferAgeSeconds,
				"next_action", "free or add capacity; serving workers retain old data until the roll succeeds")
			return true, nil
		}
		var retryable *RetryableError
		if errors.As(runErr, &retryable) && runActivation.Attempts < 3 {
			retry := retryable.RetryAfter
			if retry <= 0 {
				retry = 5 * time.Second
			}
			if err := c.store.DeferScheduleActivation(a.ID, "pending", retryable.Reason, finishedAt.Add(retry), finishedAt); err != nil {
				return true, fmt.Errorf("retry activation %d: %w", a.ID, err)
			}
			slog.Warn("schedule activation will retry", "activation_id", a.ID, "schedule_run_id", runID,
				"app", a.AppSlug, "reason", retryable.Reason, "retry_after", retry, "attempt", runActivation.Attempts)
			return true, nil
		}
		var repair *RepairRequiredError
		if errors.As(runErr, &repair) {
			retry := repair.RetryAfter
			if retry <= 0 {
				retry = 5 * time.Second
			}
			if err := c.store.DeferScheduleActivation(a.ID, "repairing", repair.Reason, finishedAt.Add(retry), finishedAt); err != nil {
				return true, fmt.Errorf("retry activation repair %d: %w", a.ID, err)
			}
			slog.Warn("schedule activation repair will retry", "activation_id", a.ID, "schedule_run_id", runID,
				"app", a.AppSlug, "reason", repair.Reason, "retry_after", retry, "attempt", runActivation.Attempts)
			return true, nil
		}
		status = "failed"
		lastError = runErr.Error()
	}
	if err := c.store.FinishScheduleActivation(a.ID, status, lastError, finishedAt, countAttempt); err != nil {
		return true, fmt.Errorf("finish activation %d: %w", a.ID, err)
	}
	slog.Info("schedule activation finished", "activation_id", a.ID, "schedule_run_id", runID,
		"app", a.AppSlug, "generation", a.TargetGeneration, "status", status, "error", lastError)
	if status == "failed" {
		return true, fmt.Errorf("activation %d for %s: %w", a.ID, a.AppSlug, runErr)
	}
	return true, nil
}

// capacityRetryDelay backs an unsatisfied host-capacity check off from one
// minute to a fifteen-minute cap. The durable deferral count makes the cadence
// survive server restarts without consuming the rollout attempt budget.
func capacityRetryDelay(base time.Duration, priorDeferrals int) time.Duration {
	if base <= 0 {
		base = time.Minute
	}
	const capDelay = 15 * time.Minute
	if base > capDelay {
		base = capDelay
	}
	delay := base
	for i := 0; i < priorDeferrals && delay < capDelay; i++ {
		delay *= 2
		if delay > capDelay {
			delay = capDelay
		}
	}
	return delay
}

// safeRoll contains a runner panic so one faulty runtime edge cannot terminate
// the only activation coordinator and strand the global queue. A panic is
// classified conservatively as repair-required: the coordinator cannot prove
// which runtime/proxy mutation completed before control unwound, so an ordinary
// supersedable retry would be unsafe even when the durable phase looks early.
func (c *Coordinator) safeRoll(ctx context.Context, a *db.ScheduleActivation) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			safego.RepanicFatal(recovered)
			err = &RepairRequiredError{
				Reason:     fmt.Sprintf("activation runner panicked: %v", recovered),
				RetryAfter: 5 * time.Second,
			}
		}
	}()
	return c.runner.Roll(ctx, a)
}
