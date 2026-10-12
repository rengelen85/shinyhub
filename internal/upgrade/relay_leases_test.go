package upgrade

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRelayLeaseSealingAndExactBackend(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	registry := RelayLeases{Dir: dir}
	lease, err := NewRelayLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if _, live, unknown := registry.Snapshot(); live != 1 || !unknown {
		t.Fatal("an unsealed live lease must be unknown")
	}
	backend := RelayBackend{Slug: "dashboard", DeploymentID: 7, Index: 0, TargetURL: "http://localhost:1234", Sessions: 2}
	if err := lease.Publish([]RelayBackend{backend}); err != nil {
		t.Fatal(err)
	}
	if n, u := registry.Sessions("dashboard", 7, 0, backend.TargetURL); n != 2 || u {
		t.Fatalf("sessions=%d unknown=%t", n, u)
	}
	if n, u := registry.Sessions("dashboard", 7, 0, "http://localhost:5678"); n != 0 || u {
		t.Fatal("replacement at same index inherited old sessions")
	}
	backend.Sessions = 1
	if err := lease.Publish([]RelayBackend{backend}); err != nil {
		t.Fatal(err)
	}
	backend.Sessions = 2
	if err := lease.Publish([]RelayBackend{backend}); err == nil {
		t.Fatal("lease increased after sealing")
	}
	backend.Sessions = 1
	backend.TargetURL = "http://localhost:5678"
	if err := lease.Publish([]RelayBackend{backend}); err == nil {
		t.Fatal("sealed lease added another backend")
	}
	if err := lease.Publish(nil); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if _, live, u := registry.Snapshot(); live != 0 || u {
		t.Fatal("closed relay still live")
	}
}

func TestRelayRegistryFailsClosedAndCollectsDeadLease(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	r := RelayLeases{Dir: dir}
	lease, err := NewRelayLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	path := strings.TrimSuffix(lease.path, ".lock") + ".json"
	for _, data := range []string{`{`, `{"protocol":2}`, `{"protocol":1,"backends":[{"slug":"a","deployment_id":1,"index":0,"target_url":"http://x","sessions":-1}]}`} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, live, u := r.Snapshot(); live != 1 || !u {
			t.Fatalf("corrupt live snapshot interpreted as idle: %s", data)
		}
	}
	dead := filepath.Join(dir, "relay-dead.lock")
	if err := os.WriteFile(dead, nil, 0600); err != nil {
		t.Fatal(err)
	}
	r.Snapshot()
	if _, err := os.Stat(dead); !os.IsNotExist(err) {
		t.Fatal("dead lease not collected")
	}
	// Oversized aggregate counts cannot wrap into a false zero or negative.
	b := RelayBackend{Slug: "a", DeploymentID: 1, Index: 0, TargetURL: "http://x", Sessions: math.MaxInt64}
	if err := lease.Publish([]RelayBackend{b, b}); err == nil {
		t.Fatal("accepted overflowing count")
	}
}

func TestRelayLeaseRejectsPublicOrSymlinkDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "public")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelayLease(dir); err == nil {
		t.Fatal("accepted public directory")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(t.TempDir(), link); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRelayLease(link); err == nil {
		t.Fatal("accepted symlink directory")
	}
}

func TestRelayPublicationCannotBeCollectedBeforeLock(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	registry := &RelayLeases{Dir: dir}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				registry.Snapshot()
			}
		}
	}()
	defer func() { close(stop); <-done }()
	backend := RelayBackend{Slug: "dashboard", DeploymentID: 7, Index: 0, TargetURL: "http://localhost:1234", Sessions: 1}
	for range 100 {
		lease, err := NewRelayLease(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := lease.Publish([]RelayBackend{backend}); err != nil {
			lease.Close()
			t.Fatal(err)
		}
		if n, unknown := registry.Sessions("dashboard", 7, 0, backend.TargetURL); n != 1 || unknown {
			lease.Close()
			t.Fatalf("live published relay became invisible: %d %t", n, unknown)
		}
		lease.Close()
	}
}
