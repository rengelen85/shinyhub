//go:build linux

package runtimesocket

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLongPrivateSocketPreservesActiveAndRecoversStaleEndpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), strings.Repeat("private-", 20), "control.sock")
	listener, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := DialContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if duplicate, err := Listen(path); err == nil {
		duplicate.Close()
		t.Fatal("replaced the live endpoint")
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Listen(path)
	if err != nil {
		t.Fatalf("cannot recover long stale endpoint: %v", err)
	}
	t.Cleanup(func() { recovered.Close() })
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("recovered endpoint permissions: %v %v", info, err)
	}
	// Reclosing the retired listener must not close a reused directory FD or
	// unlink the replacement endpoint.
	listener.Close()
	conn, err = DialContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired endpoint was not unlinked: %v", err)
	}
}

func TestListenRejectsDirectoryOwnedByAnotherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing directory ownership requires root")
	}
	dir := filepath.Join(t.TempDir(), "foreign")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(dir, 65534, 65534); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "control.sock")
	if listener, err := Listen(path); err == nil {
		listener.Close()
		t.Fatal("accepted a runtime directory another user controls")
	} else if !strings.Contains(err.Error(), "owned by the current user") {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created an endpoint in a foreign directory: %v", err)
	}
}
