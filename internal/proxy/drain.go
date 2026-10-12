package proxy

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/rvben/shinyhub/internal/safego"
	"github.com/rvben/shinyhub/internal/upgrade"
)

// connTracker is the registry of live hijacked (upgraded / WebSocket)
// connections served by this instance. A graceful shutdown uses it to wait for
// those connections to close, count them, and force-close any stragglers at the
// drain deadline. It is separate from the per-replica activeConns request
// counter (which serves hibernation) - this set tracks individual long-lived
// connections, which activeConns cannot.
type connTracker struct {
	mu     sync.Mutex
	conns  map[*trackedConn]struct{}
	sealed bool
}

func newConnTracker() *connTracker {
	return &connTracker{conns: make(map[*trackedConn]struct{})}
}

// track wraps c so that closing it unregisters it, registers the wrapper, and
// returns the wrapper to hand back to the hijacking caller. principal is the
// identity the connection was admitted under; it is required rather than
// optional because a connection nobody can attribute is a connection nobody can
// revoke.
func (t *connTracker) track(c net.Conn, principal ConnPrincipal) net.Conn {
	return t.trackWithClose(c, principal, nil)
}

func (t *connTracker) trackWithClose(c net.Conn, principal ConnPrincipal, onClose func()) net.Conn {
	return t.trackWithSession(c, principal, onClose, nil)
}

func (t *connTracker) trackWithSession(c net.Conn, principal ConnPrincipal, onClose func(), session *wsSession, backend ...upgrade.RelayBackend) net.Conn {
	tc := &trackedConn{Conn: c, tracker: t, principal: principal, onClose: onClose, session: session}
	if len(backend) == 1 {
		tc.backend = backend[0]
	}
	delay := time.Duration(0)
	deadline := principal.SessionExpiresAt
	reason := "session_expired"
	if !principal.SupportExpiresAt.IsZero() && (deadline.IsZero() || principal.SupportExpiresAt.Before(deadline)) {
		deadline = principal.SupportExpiresAt
		reason = "support_expired"
	}
	hasDeadline := !deadline.IsZero()
	if hasDeadline {
		delay = time.Until(deadline)
	}
	t.mu.Lock()
	if t.sealed {
		t.mu.Unlock()
		_ = tc.closeWithReason("relay_sealed")
		return tc
	}
	t.conns[tc] = struct{}{}
	if hasDeadline && delay > 0 {
		// Publish the timer while holding its dedicated lock. A near-zero timer
		// callback may start immediately, but Close will wait until the pointer is
		// assigned; an early manual Close can then cancel and release the closure.
		tc.timerMu.Lock()
		tc.deadlineTimer = safego.AfterFunc(delay, "proxy drain deadline close", func() { _ = tc.closeWithReason(reason) })
		tc.timerMu.Unlock()
	}
	t.mu.Unlock()
	if hasDeadline && delay <= 0 {
		_ = tc.closeWithReason(reason)
	}
	return tc
}

// snapshot returns the currently tracked connections. Callers iterate the copy
// so a Close (which takes the same lock to unregister) cannot deadlock.
func (t *connTracker) snapshot() []*trackedConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	open := make([]*trackedConn, 0, len(t.conns))
	for tc := range t.conns {
		open = append(open, tc)
	}
	return open
}

func (t *connTracker) forget(tc *trackedConn) {
	t.mu.Lock()
	delete(t.conns, tc)
	t.mu.Unlock()
}

func (t *connTracker) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

// closeAll force-closes every tracked connection and returns how many were
// open. Each Close unregisters its own entry (via trackedConn.Close), so the
// set is empty afterward.
func (t *connTracker) closeAll() int {
	open := t.snapshot()
	for _, tc := range open {
		_ = tc.closeWithReason("drain")
	}
	return len(open)
}

// trackedConn wraps a hijacked net.Conn so the first Close unregisters it from
// the tracker. All other net.Conn behaviour is inherited unchanged. Close is
// fully idempotent: the underlying conn is closed exactly once and the cached
// error is returned on every subsequent call.
type trackedConn struct {
	net.Conn
	tracker *connTracker
	// principal is fixed at the upgrade and never mutated, so the recheck sweep
	// reads it without holding the tracker lock.
	principal     ConnPrincipal
	backend       upgrade.RelayBackend
	onClose       func()
	session       *wsSession
	once          sync.Once
	closeErr      error
	timerMu       sync.Mutex
	deadlineTimer *time.Timer
}

func (c *trackedConn) Read(p []byte) (int, error) {
	if c.session != nil {
		c.session.beginIO()
	}
	n, err := c.Conn.Read(p)
	if c.session != nil {
		c.session.finishIO("client", n, err, true, p[:n])
	}
	return n, err
}

func (c *trackedConn) Write(p []byte) (int, error) {
	if c.session != nil {
		c.session.beginIO()
	}
	n, err := c.Conn.Write(p)
	if c.session != nil {
		c.session.finishIO("client", n, err, false, nil)
	}
	return n, err
}

func (c *trackedConn) closeWithReason(reason string) error {
	return c.close(reason)
}

func (c *trackedConn) Close() error {
	return c.close("")
}

func (c *trackedConn) close(reason string) error {
	c.once.Do(func() {
		if c.session != nil {
			c.session.beginClientClose(reason)
		}
		c.timerMu.Lock()
		if c.deadlineTimer != nil {
			c.deadlineTimer.Stop()
			c.deadlineTimer = nil
		}
		c.timerMu.Unlock()
		c.tracker.forget(c)
		c.closeErr = c.Conn.Close()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.closeErr
}

// SetDraining marks (or unmarks) this instance as draining for shutdown. While
// draining, /readyz reports unready so a load balancer stops routing new
// requests; existing hijacked connections keep flowing.
func (p *Proxy) SetDraining(v bool) { p.instanceDraining.Store(v) }

// Draining reports whether this instance is draining for shutdown.
func (p *Proxy) Draining() bool { return p.instanceDraining.Load() }

// SetRetiringParent fences destructive lifecycle decisions while the proxy
// which served established sessions before this handoff is still alive and
// until its recovery cleanup completes. Close done only after both conditions.
// Call before recovery or request handling; a nil channel means no parent.
func (p *Proxy) SetRetiringParent(done <-chan struct{}) { p.retiringParent = done }

func (p *Proxy) RetiringParentAlive() bool {
	if p.relayLeases != nil {
		if p.relayLeases.Unknown() {
			return true
		}
	}
	if p.retiringParent == nil {
		return false
	}
	select {
	case <-p.retiringParent:
		return false
	default:
		return true
	}
}

// SetRelayLeases must precede recovery and serving. A relay's sealed snapshot
// cannot gain sessions, so stale counts conservatively protect its backend.
func (p *Proxy) SetRelayLeases(leases *upgrade.RelayLeases) { p.relayLeases = leases }

func (p *Proxy) relaySessions(slug string, deploymentID int64, index int, target string) int64 {
	if p.relayLeases == nil {
		return 0
	}
	n, unknown := p.relayLeases.Sessions(slug, deploymentID, index, target)
	if unknown {
		return 1 << 31
	}
	return n
}

func (p *Proxy) HasRelaySessions(slug string, deploymentID int64) bool {
	return p.relaySessions(slug, deploymentID, -1, "") > 0
}

// RelayUpgradeBlockers names pools that cannot safely enter relay mode. The
// second result is true only when every blocker can clear without changing
// configuration; elastic pools are permanent blockers.
func (p *Proxy) RelayUpgradeBlockers() ([]string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var blockers []string
	temporary := true
	for slug, pool := range p.pools {
		if poolIsElastic(pool) {
			temporary = false
			blockers = append(blockers, fmt.Sprintf("%s: elastic pool", slug))
		}
		if len(pool.drainingGenerations) > 0 {
			blockers = append(blockers, fmt.Sprintf("%s: draining generation", slug))
		}
		if len(pool.candidates) > 0 {
			blockers = append(blockers, fmt.Sprintf("%s: candidate generation", slug))
		}
	}
	sort.Strings(blockers)
	return blockers, temporary
}

// RelaySnapshot returns exact upgraded-connection identities. Only multiplex
// pools with a stable active generation can enter indefinite relay mode;
// transitions keep the ordinary bounded retirement protocol.
func (p *Proxy) RelaySnapshot() ([]upgrade.RelayBackend, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.pools {
		if poolIsElastic(pool) || len(pool.drainingGenerations) > 0 || len(pool.candidates) > 0 {
			return nil, false
		}
	}
	counts := make(map[upgrade.RelayBackend]int64)
	for _, conn := range p.conns.snapshot() {
		key := conn.backend
		if key.Slug == "" || key.TargetURL == "" || key.DeploymentID <= 0 || key.Index < 0 {
			return nil, false
		}
		pool := p.pools[key.Slug]
		if pool == nil || key.Index >= len(pool.replicas) {
			return nil, false
		}
		replica := pool.replicas[key.Index]
		if replica == nil || replica.draining.Load() || replica.deploymentID != key.DeploymentID || replica.targetURL != key.TargetURL {
			return nil, false
		}
		counts[key]++
	}
	out := make([]upgrade.RelayBackend, 0, len(counts))
	for key, count := range counts {
		key.Sessions = count
		out = append(out, key)
	}
	return out, true
}

// SealRelay atomically prevents any later hijack from enlarging the lease.
// HTTP must already be quiescent: an unexpected late hijack is closed instead
// of admitting unprotected work. Subsequent snapshots use immutable conn keys.
func (p *Proxy) SealRelay() ([]upgrade.RelayBackend, bool) {
	_, stable := p.RelaySnapshot()
	if !stable {
		return nil, false
	}
	p.conns.mu.Lock()
	p.conns.sealed = true
	p.conns.mu.Unlock()
	return p.sealedRelaySnapshot(), true
}
func (p *Proxy) sealedRelaySnapshot() []upgrade.RelayBackend {
	counts := make(map[upgrade.RelayBackend]int64)
	for _, conn := range p.conns.snapshot() {
		counts[conn.backend]++
	}
	out := make([]upgrade.RelayBackend, 0, len(counts))
	for backend, count := range counts {
		backend.Sessions = count
		out = append(out, backend)
	}
	return out
}

// PreserveUpgraded has no reload deadline. Service shutdown still applies the
// ordinary bounded drain, and access rechecks continue independently.
func (p *Proxy) PreserveUpgraded(ctx context.Context, lease *upgrade.RelayLease, shutdownGrace time.Duration) int {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for p.ActiveUpgradedConns() > 0 {
		select {
		case <-ctx.Done():
			return p.DrainUpgraded(shutdownGrace)
		case <-ticker.C:
			backends := p.sealedRelaySnapshot()
			if lease.Publish(backends) != nil {
				// Keep the last conservative snapshot; never revoke backend
				// protection merely because a snapshot update failed.
				continue
			}
		}
	}
	return 0
}

// ActiveUpgradedConns is the number of live hijacked (WebSocket) connections.
func (p *Proxy) ActiveUpgradedConns() int { return p.conns.count() }

func (p *Proxy) UpgradedPrincipals() []ConnPrincipal {
	set := make(map[ConnPrincipal]bool)
	for _, conn := range p.conns.snapshot() {
		set[conn.principal] = true
	}
	out := make([]ConnPrincipal, 0, len(set))
	for principal := range set {
		out = append(out, principal)
	}
	return out
}

// CloseUnverifiedUpgraded ends sessions whose retiring proxy can no longer
// renew authorization through the current owner. It does not bypass ordinary
// service-shutdown grace or a session's immutable login deadline.
func (p *Proxy) CloseUnverifiedUpgraded() int {
	open := p.conns.snapshot()
	for _, conn := range open {
		_ = conn.closeWithReason("authorization_unverifiable")
	}
	return len(open)
}

// DrainUpgraded waits for all tracked upgraded connections to close on their
// own, up to timeout, then force-closes any that remain. It returns the number
// force-closed (0 means everything drained cleanly). It returns immediately
// when no upgraded connections are open. Callers are expected to have called
// SetDraining(true) and stopped accepting new connections before calling
// DrainUpgraded, so the tracked set only shrinks.
func (p *Proxy) DrainUpgraded(timeout time.Duration) (forced int) {
	return p.DrainUpgradedContext(context.Background(), timeout)
}

// DrainUpgradedContext also ends a retiring proxy's drain when service shutdown
// is requested. This prevents a long reload-only budget from delaying SIGTERM.
// Reauthorization may close connections concurrently; trackedConn.Close is
// idempotent, so those connections retain their normal revocation semantics.
func (p *Proxy) DrainUpgradedContext(ctx context.Context, timeout time.Duration) (forced int) {
	return p.drainUpgraded(ctx, timeout, 0)
}

// DrainUpgradedForReload shrinks a reload drain to the ordinary shutdown grace
// when SIGINT/SIGTERM arrives, without disconnecting sessions immediately.
func (p *Proxy) DrainUpgradedForReload(ctx context.Context, timeout, shutdownGrace time.Duration) int {
	return p.drainUpgraded(ctx, timeout, shutdownGrace)
}

func (p *Proxy) drainUpgraded(ctx context.Context, timeout, shutdownGrace time.Duration) int {
	if p.conns.count() == 0 {
		return 0
	}
	deadline := time.NewTimer(max(0, timeout))
	expires := time.Now().Add(timeout)
	shutdown := ctx.Done()
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for p.conns.count() > 0 {
		select {
		case <-shutdown:
			if shutdownGrace <= 0 {
				return p.conns.closeAll()
			}
			deadline.Reset(max(0, min(time.Until(expires), shutdownGrace)))
			shutdown = nil
		case <-deadline.C:
			return p.conns.closeAll()
		case <-ticker.C:
		}
	}
	return 0
}
