package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/access"
	"github.com/rvben/shinyhub/internal/db"
	"github.com/rvben/shinyhub/internal/dbtest"
	"github.com/rvben/shinyhub/internal/proxy"
	"github.com/rvben/shinyhub/internal/upgrade"
)

func TestRelayOwnerSocketDoesNotContaminateLeaseNamespace(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "relay-reg-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	owner, err := startRelayOwner(dir, "secret", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if filepath.Dir(filepath.Dir(owner.path)) != dir {
		t.Fatalf("IPC should use the short private registry: %s", owner.path)
	}
	if err := owner.Activate(); err != nil {
		t.Fatal(err)
	}
	leases := &upgrade.RelayLeases{Dir: dir}
	if _, live, unknown := leases.Snapshot(); live != 0 || unknown {
		t.Fatalf("owner metadata is not a session lease: live=%d unknown=%v", live, unknown)
	}
	lease, err := upgrade.NewRelayLease(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	backend := upgrade.RelayBackend{Slug: "dashboard", DeploymentID: 1, Index: 0, TargetURL: "http://localhost:1234", Sessions: 2}
	if err := lease.Publish([]upgrade.RelayBackend{backend}); err != nil {
		t.Fatal(err)
	}
	if backends, live, unknown := leases.Snapshot(); live != 1 || unknown || len(backends) != 1 || backends[0] != backend {
		t.Fatalf("exact live lease survives owner metadata: backends=%v live=%d unknown=%v", backends, live, unknown)
	}
}

func TestRelayIPCDirectoryPrefersDurablePrivatePaths(t *testing.T) {
	base, err := os.MkdirTemp("/tmp", "relay-ipc-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	registry := filepath.Join(base, "r")
	if err := os.Mkdir(registry, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUNTIME_DIRECTORY", base)
	dir, err := relayIPCDirectory(registry)
	if err != nil || filepath.Dir(dir) != registry {
		t.Fatalf("short private registry: directory=%q err=%v", dir, err)
	}
	dir, err = relayIPCDirectory("/" + strings.Repeat("long", 40))
	if err != nil || filepath.Dir(dir) != base {
		t.Fatalf("runtime fallback: directory=%q err=%v", dir, err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err = relayIPCDirectory("/" + strings.Repeat("long", 40))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if filepath.Dir(dir) != "/tmp" {
		t.Fatalf("public runtime directory must be skipped: %q", dir)
	}
}

func TestRelayOwnerPublicationAndReplacement(t *testing.T) {
	// Use the short IPC directory even when the registry is deeply nested.
	dir, err := relayIPCDirectory(filepath.Join(t.TempDir(), "registry"))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	principal := proxy.ConnPrincipal{}
	owner, err := startRelayOwner(dir, "secret", nil, func(proxy.ConnPrincipal) (bool, string, error) { return false, "", nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := owner.Activate(); err != nil {
		t.Fatal(err)
	}
	owner.Commit()
	client := newRelayClient(dir, "secret")
	if result, err := client.authorize([]proxy.ConnPrincipal{principal}); err != nil || result[principal] {
		t.Fatalf("authorization: %v %v", result, err)
	}
	failed, err := startRelayOwner(dir, "secret", nil, func(proxy.ConnPrincipal) (bool, string, error) { return true, "revoked", nil })
	if err != nil {
		t.Fatal(err)
	}
	failed.Close()
	if result, err := client.authorize([]proxy.ConnPrincipal{principal}); err != nil || result[principal] {
		t.Fatal("unready child replaced current owner")
	}
	next, err := startRelayOwner(dir, "secret", nil, func(proxy.ConnPrincipal) (bool, string, error) { return true, "revoked", nil })
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if err := next.Activate(); err != nil {
		t.Fatal(err)
	}
	next.Commit()
	owner.Close()
	if result, err := client.authorize([]proxy.ConnPrincipal{principal}); err != nil || !result[principal] {
		t.Fatalf("successor publication lost: %v %v", result, err)
	}
	bad := newRelayClient(dir, "wrong")
	if _, err := bad.authorize([]proxy.ConnPrincipal{principal}); err == nil {
		t.Fatal("incorrect MAC accepted")
	}
	next.Close()
	if _, err := client.authorize([]proxy.ConnPrincipal{principal}); err == nil {
		t.Fatal("closed owner renewed authorization")
	}
}

func TestRelayAuthorizationDeadlineIndependentOfRPC(t *testing.T) {
	// A stalled authorization RPC must not delay the independent expiry timer.
	client := &relayClient{secret: "secret", client: &http.Client{Transport: relayStallingTransport{}, Timeout: time.Second}}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runRelayAuthorization(ctx, proxy.New(), client, 10*time.Millisecond, 40*time.Millisecond)
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("authorization expiry waited for RPC: %v", elapsed)
	}
}

type relayStallingTransport struct{}

func (relayStallingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-time.After(200 * time.Millisecond):
		return nil, context.DeadlineExceeded
	}
}

func TestFailedReadyRestoresPreviousRelayOwner(t *testing.T) {
	dir, err := relayIPCDirectory(filepath.Join(t.TempDir(), "registry"))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	check := func(proxy.ConnPrincipal) (bool, string, error) { return false, "", nil }
	owner, err := startRelayOwner(dir, "secret", nil, check)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := owner.Activate(); err != nil {
		t.Fatal(err)
	}
	owner.Commit()
	failed, err := startRelayOwner(dir, "secret", nil, check)
	if err != nil {
		t.Fatal(err)
	}
	if err := failed.Activate(); err != nil {
		t.Fatal(err)
	}
	failed.Close()
	if _, err := newRelayClient(dir, "secret").authorize([]proxy.ConnPrincipal{{Slug: "dashboard"}}); err != nil {
		t.Fatalf("failed ready lost previous owner: %v", err)
	}
}

func TestRelayDeletedPrincipalDoesNotBlockOtherVerdicts(t *testing.T) {
	store := dbtest.New(t)
	if err := store.CreateUser(db.CreateUserParams{Username: "owner", PasswordHash: "unused", Role: "admin"}); err != nil {
		t.Fatal(err)
	}
	user, err := store.GetUserByUsername("owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateApp(db.CreateAppParams{Slug: "dashboard", Name: "Dashboard", OwnerID: user.ID, Access: "public"}); err != nil {
		t.Fatal(err)
	}
	dir, err := relayIPCDirectory("")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	check := func(p proxy.ConnPrincipal) (bool, string, error) {
		return access.Recheck(store, store.LookupContextUser, store.IsTokenRevoked, access.Principal{Slug: p.Slug, UserID: p.UserID, Role: p.Role, SessionEpoch: p.SessionEpoch})
	}
	owner, err := startRelayOwner(dir, "secret", store, check)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := owner.Activate(); err != nil {
		t.Fatal(err)
	}
	owner.Commit()
	removed := proxy.ConnPrincipal{Slug: "dashboard", UserID: 99999, Role: "viewer"}
	valid := proxy.ConnPrincipal{Slug: "dashboard", UserID: user.ID, Role: user.Role, SessionEpoch: user.TokenEpoch}
	verdicts, err := newRelayClient(dir, "secret").authorize([]proxy.ConnPrincipal{removed, valid})
	if err != nil || !verdicts[removed] || verdicts[valid] {
		t.Fatalf("deleted principal spoiled batch: %v %v", verdicts, err)
	}
}
