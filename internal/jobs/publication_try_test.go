package jobs

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"testing"
	"time"
)

func TestPublicationTryReadYieldsToActiveAndQueuedWriters(t *testing.T) {
	gate := &publicationGate{}
	if !gate.TryRLock() {
		t.Fatal("idle gate rejected reader")
	}
	gate.RUnlock()
	gate.RWMutex.Lock()
	if gate.TryRLock() {
		t.Fatal("reader passed active writer")
	}
	gate.RWMutex.Unlock()
	// A queued writer holds admission until its exclusive read/write lock can
	// be taken. Try readers must not overtake it even while only readers run.
	gate.RWMutex.RLock()
	gate.admission.Lock()
	if gate.TryRLock() {
		t.Fatal("reader overtook queued writer")
	}
	gate.admission.Unlock()
	gate.RWMutex.RUnlock()
	if !gate.TryRLock() {
		t.Fatal("failed try leaked admission")
	}
	gate.RUnlock()
}

func TestTryCompatibleBootGateHonorsCrossProcessWriter(t *testing.T) {
	m := &Manager{publicationGates: map[int64]*publicationGate{}, publicationLockDir: t.TempDir()}
	release, _, err := m.acquirePublicationFileLock(1, unix.LOCK_EX) // independent file description
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = m.TryAcquireCompatibleConsumerBootGate(1)
	release()
	if !errors.Is(err, ErrConsumerBootBusy) || time.Since(start) > time.Second {
		t.Fatalf("busy gate: %v", err)
	}
	// Prove the rejected try released both the local read side and its file.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	writer, _, err := m.acquirePublicationWriter(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer()
}
