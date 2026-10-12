package process

import (
	"context"
	"testing"
	"time"
)

func TestRelayOutputWaitIgnoresAdoptedAndJoinsOwnedBackends(t *testing.T) {
	ownedDone := make(chan struct{})
	adoptedDone := make(chan struct{})
	m := &Manager{entries: map[string][]*entry{"dashboard": {{ownsOutput: true, done: ownedDone}, {done: adoptedDone}}}}
	done := make(chan struct{})
	go func() { m.WaitOwnedOutput(context.Background()); close(done) }()
	select {
	case <-done:
		t.Fatal("owned stdout pipe abandoned before backend exit")
	case <-time.After(20 * time.Millisecond):
	}
	close(ownedDone)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("adopted backend accumulated another output pump")
	}
}

func TestRelayOutputWaitStopsAtServiceShutdown(t *testing.T) {
	m := &Manager{entries: map[string][]*entry{"dashboard": {{ownsOutput: true, done: make(chan struct{})}}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { m.WaitOwnedOutput(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("service shutdown left relay waiting for output")
	}
}
