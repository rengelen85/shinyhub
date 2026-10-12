package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rvben/shinyhub/internal/db"
)

func TestRetiringParentFenceHeldUntilCleanupCompletes(t *testing.T) {
	exited := make(chan struct{})
	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan struct{})
	var fenced atomic.Bool
	fenced.Store(true)
	go func() {
		defer close(done)
		runRetiringParentCleanup(context.Background(), exited, []*db.App{{Slug: "app"}},
			func(string) (func(), bool) { return func() {}, true },
			func(*db.App) error { close(entered); <-finish; return nil },
			func() { fenced.Store(false) })
	}()
	close(exited)
	<-entered
	if !fenced.Load() {
		t.Fatal("lifecycle fence opened before cleanup completed")
	}
	close(finish)
	<-done
	if fenced.Load() {
		t.Fatal("lifecycle fence remained after cleanup completed")
	}
}

func TestRetiringParentCleanupCancellationKeepsFence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	close(exited)
	runRetiringParentCleanup(ctx, exited, []*db.App{{Slug: "app"}},
		func(string) (func(), bool) { cancel(); return nil, false },
		func(*db.App) error { t.Fatal("cleanup ran without lock"); return nil },
		func() { t.Fatal("cancelled owner released lifecycle fence") })
}
