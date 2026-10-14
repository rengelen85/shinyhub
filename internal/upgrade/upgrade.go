// Package upgrade provides listener-preserving process handoffs and negotiated
// control-plane retirement for session relays. The descriptor protocol remains
// compatible with tableflip for the first upgrade from older binaries.
package upgrade

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Upgrader is the listener handoff contract used by the server.
type Upgrader interface {
	// Listen returns a listener whose underlying socket survives an upgrade
	// (inherited by the successor process). All Listen calls must precede Ready.
	Listen(network, addr string) (net.Listener, error)
	// Ready signals this process is serving: it closes inherited-but-unused
	// fds, writes the PID file, and tells the parent (if this is an upgrade
	// child) that it may exit. Call once after every Listen and after the
	// servers are accepting.
	Ready() error
	// Upgrade spawns the new binary, hands it the listener fds, and blocks until
	// it calls Ready (or UpgradeTimeout elapses). On success the caller should
	// shut down; on error the caller keeps serving.
	Upgrade() error
	// Exit is closed when this process should shut down: after Stop, or after a
	// successful Upgrade handed off to a ready successor.
	Exit() <-chan struct{}
	// HasParent reports whether this process was started by an upgrade handoff
	// and therefore inherits its listeners from a parent that is still bound.
	HasParent() bool
	// WaitForParent returns when the parent retires its control plane. Legacy
	// parents signal only at process exit.
	WaitForParent(context.Context) error
	// Stop disables further upgrades and closes Exit.
	Stop()
}

// New returns a listener-preserving Upgrader. timeout bounds the wait for a
// successor to become Ready during an upgrade; pidFile, when non-empty, receives
// the ready process's PID (systemd MAINPID tracking).
func New(timeout time.Duration, pidFile string) (Upgrader, error) {
	return NewRepeatable(timeout, pidFile)
}

// SignalState records whether shutdown follows a successful upgrade.
type SignalState struct {
	mu       sync.Mutex
	inFlight chan struct{}
	handoff  bool
	waitHook func() func(bool)
}

// SetReloadWaitHook defers new background work during an acknowledged wait.
// Completion receives true only after a successful handoff, when the retiring
// process must keep its background admissions closed.
func (s *SignalState) SetReloadWaitHook(hook func() func(bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waitHook = hook
}

// TransitionError identifies a temporary admission failure. Waiting reloads
// retry only these failures; configuration and successor failures stay fatal.
type TransitionError struct{ Reason string }

func (e *TransitionError) Error() string { return e.Reason }

// Reload serializes signals and acknowledged reload requests. It never queues
// another handoff behind an in-flight or already successful handoff.
func (s *SignalState) Reload(ctx context.Context, upg Upgrader, wait time.Duration, log *slog.Logger) error {
	s.mu.Lock()
	if s.handoff || s.inFlight != nil {
		s.mu.Unlock()
		return errors.New("reload already in progress or this process has handed off")
	}
	done := make(chan struct{})
	s.inFlight = done
	hook := s.waitHook
	s.mu.Unlock()
	succeeded := false
	if wait > 0 && hook != nil {
		finish := hook()
		if finish != nil {
			defer func() { finish(succeeded) }()
		}
	}
	defer func() {
		s.mu.Lock()
		s.handoff = s.handoff || succeeded
		close(done)
		s.inFlight = nil
		s.mu.Unlock()
	}()
	deadline := time.Now().Add(wait)
	var lastReason string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := upg.Upgrade()
		if err == nil {
			succeeded = true
			return nil
		}
		var transition *TransitionError
		if wait == 0 || !errors.As(err, &transition) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("reload wait timed out: %w", err)
		}
		if lastReason != err.Error() {
			log.Info("waiting to reload", "reason", err, "remaining", remaining)
			lastReason = err.Error()
		}
		delay := min(time.Second, remaining)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("reload canceled while waiting (%s): %w", err, ctx.Err())
		case <-timer.C:
		}
	}
}

// SuccessfulHandoff waits for any pending Upgrade result. The library may
// close Exit before the signal goroutine has received that result.
func (s *SignalState) SuccessfulHandoff() bool {
	s.mu.Lock()
	done := s.inFlight
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.handoff
}

// WireSignals connects process signals to the upgrader lifecycle and returns
// immediately (it launches one goroutine per concern):
//   - each value on sighup triggers a zero-downtime Upgrade; a failed upgrade is
//     logged and the current process keeps serving.
//   - ctx cancellation (SIGINT/SIGTERM via signal.NotifyContext) calls Stop,
//     which closes Exit so the main loop proceeds to graceful shutdown.
func WireSignals(ctx context.Context, upg Upgrader, sighup <-chan os.Signal, log *slog.Logger) *SignalState {
	state := &SignalState{}
	// SIGHUP loop: triggers an upgrade per signal, and exits on ctx cancellation
	// so the goroutine does not outlive the server.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-upg.Exit():
				return
			case _, ok := <-sighup:
				if !ok {
					return
				}
				log.Info("SIGHUP received, starting zero-downtime upgrade")
				err := state.Reload(ctx, upg, 0, log)
				if err != nil {
					log.Error("zero-downtime upgrade failed; continuing to serve", "err", err)
					continue
				}
				log.Info("zero-downtime upgrade succeeded; successor is ready")
				return // A retiring relay must never handle another upgrade signal.
			}
		}
	}()
	// Dedicated Stop goroutine so a SIGINT/SIGTERM closes Exit promptly even if an
	// upgrade is in flight in the loop above.
	go func() {
		<-ctx.Done()
		upg.Stop()
	}()
	return state
}

// NotifyReady announces server readiness to systemd. Supervised services keep
// MAINPID on their stable parent; unsupervised services announce their own PID.
// It is a no-op without NOTIFY_SOCKET. Call once after Upgrader.Ready().
func NotifyReady() error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return nil
	}
	// Abstract namespace sockets are encoded with a leading '@' in NOTIFY_SOCKET.
	if strings.HasPrefix(sock, "@") {
		sock = "\x00" + sock[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		return fmt.Errorf("sd_notify dial: %w", err)
	}
	defer conn.Close()
	mainPID := os.Getpid()
	if value := os.Getenv(supervisorPIDEnv); value != "" {
		pid, err := strconv.Atoi(value)
		if err != nil || pid <= 1 {
			return errors.New("invalid supervisor PID")
		}
		mainPID = pid
	}
	if _, err := fmt.Fprintf(conn, "READY=1\nMAINPID=%d\n", mainPID); err != nil {
		return fmt.Errorf("sd_notify write: %w", err)
	}
	return nil
}
