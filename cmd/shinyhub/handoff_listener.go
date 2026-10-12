package main

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// A standard Server.Shutdown can close a connection accepted just before it
// enters shutdown, before c.serve reads that client's first request. Stop the
// accept loop first, then let already-accepted connections reach their first
// request (or their header timeout) before setting the server shutdown flag.
type handoffListener struct {
	net.Listener
	mu        sync.Mutex
	pending   map[net.Conn]struct{}
	changed   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    bool
}

func newHandoffListener(ln net.Listener, srv *http.Server) *handoffListener {
	h := &handoffListener{Listener: ln, pending: make(map[net.Conn]struct{}), changed: make(chan struct{}, 1), done: make(chan struct{})}
	previous := srv.ConnState
	srv.ConnState = func(conn net.Conn, state http.ConnState) {
		if previous != nil {
			previous(conn, state)
		}
		if state != http.StateNew {
			h.mu.Lock()
			delete(h.pending, conn)
			h.mu.Unlock()
			select {
			case h.changed <- struct{}{}:
			default:
			}
		}
	}
	return h
}
func (h *handoffListener) Accept() (net.Conn, error) {
	conn, err := h.Listener.Accept()
	if err == nil {
		h.mu.Lock()
		h.pending[conn] = struct{}{}
		h.mu.Unlock()
	}
	return conn, err
}
func (h *handoffListener) Close() error {
	var err error
	h.closeOnce.Do(func() { h.mu.Lock(); h.closed = true; h.mu.Unlock(); err = h.Listener.Close() })
	return err
}
func (h *handoffListener) Closing() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.closed }
func (h *handoffListener) ServeDone()    { close(h.done) }
func (h *handoffListener) Quiesce(ctx context.Context) error {
	_ = h.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.done:
	}
	for {
		h.mu.Lock()
		remaining := len(h.pending)
		h.mu.Unlock()
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.changed:
		}
	}
}
