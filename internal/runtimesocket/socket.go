// Package runtimesocket manages private, process-local Unix control sockets.
package runtimesocket

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Listen recovers only provably stale sockets. Active listeners, regular files,
// symlinks and uncertain dial failures are preserved and fail closed.
func Listen(path string) (*net.UnixListener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("runtime control directory must be private (mode 0700)")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("runtime control path already exists and is not a socket")
		}
		c, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			c.Close()
			return nil, errors.New("runtime control socket is already active")
		}
		if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("check existing runtime socket: %w", err)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove stale runtime socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// Accept retries errors such as descriptor exhaustion with bounded backoff.
// Closing the listener terminates the loop, including during shutdown.
func Accept(listener *net.UnixListener, log *slog.Logger) (*net.UnixConn, error) {
	return accept(listener.AcceptUnix, log, time.Sleep)
}

func accept(next func() (*net.UnixConn, error), log *slog.Logger, wait func(time.Duration)) (*net.UnixConn, error) {
	delay := 5 * time.Millisecond
	for {
		conn, err := next()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return conn, err
		}
		log.Warn("runtime socket accept failed; retrying", "err", err, "retry_in", delay)
		wait(delay)
		delay = min(delay*2, time.Second)
	}
}
