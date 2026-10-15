package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

// Running-writer fences and transitional snapshots are rechecked within a
// deadline. Failed producers, deployment barriers and missing authoritative
// fields retain their ordinary failure path. Transport/5xx errors are retried.
// The caller supplies the shared per-app warm deadline; new writers never
// extend it, and waiting does not dispatch or restart producers.
func waitForAppCompatibilityClear(ctx context.Context, cfg *cliConfig, slug string, res *applyResult, out io.Writer) error {
	_, err := waitForAppCompatibilityClearObserved(ctx, cfg, slug, res, out)
	return err
}

func waitForAppCompatibilityClearObserved(ctx context.Context, cfg *cliConfig, slug string, res *applyResult, out io.Writer) (bool, error) {
	deadline, bounded := ctx.Deadline()
	var lastDetail string
	var waited bool
	timeoutError := func() (bool, error) {
		res.failureKind = failureWarmWaitTimeout
		detail := "compatibility was not confirmed"
		if lastDetail != "" {
			detail = lastDetail
		}
		return waited, fmt.Errorf("%s: compatibility was not confirmed by --warm-timeout deadline %s (%s): %w", slug, deadline.Format(time.RFC3339), detail, context.DeadlineExceeded)
	}
	for {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return timeoutError()
		}
		if err := ctx.Err(); err != nil {
			res.failureKind = failureWarmStateUnavailable
			return waited, err
		}
		err := requireAppCompatibilityClearContext(ctx, cfg, slug)
		if err == nil {
			return waited, nil
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return timeoutError()
		}
		if ctx.Err() != nil {
			res.failureKind = failureWarmStateUnavailable
			return waited, ctx.Err()
		}
		var quarantine *appCompatibilityQuarantineError
		waitable := errors.As(err, &quarantine) && (quarantine.producerInProgress || quarantine.recheckCompatibility)
		var httpErr *deployHTTPError
		var networkErr net.Error
		transient := (errors.As(err, &httpErr) && httpErr.statusCode >= 500) ||
			(errors.As(err, &networkErr) && networkErr.Timeout()) ||
			errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
			errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		var protocol *protocolError
		transient = transient && !errors.As(err, &protocol)
		if !bounded || (!waitable && !transient) {
			classifyAppCompatibilityFailure(res, err)
			return waited, err
		}
		phase, detail := "Waiting for producers", err.Error()
		if transient {
			phase, detail = "Compatibility unavailable; retrying", "compatibility status unavailable; retrying: "+err.Error()
		}
		if !updateFleetProgress(out, phase, slug+" · "+detail, deadline, transient) && lastDetail != detail {
			fmt.Fprintf(out, "  %s: %s (deadline %s)\n", slug, detail, deadline.Format(time.RFC3339))
		}
		lastDetail, waited = detail, true
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return timeoutError()
			}
			res.failureKind = failureWarmStateUnavailable
			return waited, ctx.Err()
		case <-timer.C:
		}
	}
}
