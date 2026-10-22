package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandoffWaitsForHandlerRatherThanActiveState(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	type contextKey struct{}
	stateCalls := 0
	srv := &http.Server{
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return context.WithValue(ctx, contextKey{}, "preserved")
		},
		ConnState: func(net.Conn, http.ConnState) { stateCalls++ },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Context().Value(contextKey{}) != "preserved" {
				t.Error("existing connection context lost")
			}
			fmt.Fprint(w, "served")
		}),
	}
	ln := newHandoffListener(nil, srv)
	ln.pending[conn] = struct{}{}
	srv.ConnState(conn, http.StateActive)
	if len(ln.pending) != 1 {
		t.Fatal("active state released connection before the server's shutdown check")
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(srv.ConnContext(req.Context(), conn))
	response := httptest.NewRecorder()
	srv.Handler.ServeHTTP(response, req)
	if len(ln.pending) != 0 || response.Body.String() != "served" || stateCalls != 1 {
		t.Fatalf("handler handoff failed: pending=%d body=%q state calls=%d", len(ln.pending), response.Body.String(), stateCalls)
	}
	for _, state := range []http.ConnState{http.StateClosed, http.StateHijacked, http.StateIdle} {
		ln.pending[conn] = struct{}{}
		srv.ConnState(conn, state)
		if len(ln.pending) != 0 {
			t.Fatalf("terminal state %v left a pending connection", state)
		}
	}
}

func TestHandoffQuiesceCompletesAlreadyAcceptedRequest(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "served") }), ReadHeaderTimeout: time.Second}
	ln := newHandoffListener(raw, srv)
	go func() { defer ln.ServeDone(); srv.Serve(ln) }()
	defer srv.Close()
	conn, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	deadline := time.Now().Add(time.Second)
	for {
		ln.mu.Lock()
		pending := len(ln.pending)
		ln.mu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		if err := ln.Quiesce(ctx); err != nil {
			done <- err
			return
		}
		done <- srv.Shutdown(ctx)
	}()
	select {
	case err := <-done:
		t.Fatalf("shutdown discarded first request: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != 200 || string(body) != "served" {
		t.Fatalf("accepted request lost: %d %s %v", response.StatusCode, body, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
