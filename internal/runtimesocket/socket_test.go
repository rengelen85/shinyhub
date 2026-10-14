//go:build linux || darwin

package runtimesocket

import (
	"bytes"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func testPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "shrs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "control.sock")
}

func TestListenRecoversStaleSocketAndPreservesActiveSocket(t *testing.T) {
	path := testPath(t)
	old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false)
	old.Close() // Simulate the endpoint left by SIGKILL.
	current, err := Listen(path)
	if err != nil {
		t.Fatalf("stale recovery: %v", err)
	}
	defer current.Close()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions: %v", err)
	}
	if second, err := Listen(path); err == nil {
		second.Close()
		t.Fatal("replaced active socket")
	}
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("damaged active socket: %v", err)
	}
	c.Close()
}

func TestListenPreservesNonSocketPaths(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "symlink"}[symlink], func(t *testing.T) {
			path := testPath(t)
			target := path
			if symlink {
				target = path + ".target"
			}
			if err := os.WriteFile(target, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			if symlink {
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			if listener, err := Listen(path); err == nil {
				listener.Close()
				t.Fatal("overwrote non-socket path")
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "preserve" {
				t.Fatal("damaged target")
			}
		})
	}
}

func TestAcceptRetriesDescriptorExhaustionAndStopsOnClose(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	expected := new(net.UnixConn)
	attempts := 0
	waits := 0
	conn, err := accept(func() (*net.UnixConn, error) {
		attempts++
		if attempts <= 12 {
			return nil, &net.OpError{Op: "accept", Err: syscall.EMFILE}
		}
		return expected, nil
	}, logger, func(d time.Duration) {
		waits++
		if d < 5*time.Millisecond || d > time.Second {
			t.Fatal("unbounded accept backoff")
		}
	})
	if err != nil || conn != expected || waits != 12 || !bytes.Contains(logs.Bytes(), []byte("retrying")) {
		t.Fatalf("retry failed: waits=%d error=%v", waits, err)
	}
	_, err = accept(func() (*net.UnixConn, error) { return nil, &net.OpError{Op: "accept", Err: net.ErrClosed} }, logger, func(time.Duration) { t.Fatal("retry after close") })
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close error lost: %v", err)
	}
}
