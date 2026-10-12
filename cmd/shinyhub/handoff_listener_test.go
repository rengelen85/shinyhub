package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

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
