//go:build linux

package upgrade

import (
	"fmt"
	"golang.org/x/sys/unix"
	"syscall"
)

type serverPIDWatcher struct{ fd int }

func watchServerPID(pid int) (*serverPIDWatcher, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("supervisor requires Linux pidfd support (kernel 5.3+): %w", err)
	}
	return &serverPIDWatcher{fd: fd}, nil
}
func (w *serverPIDWatcher) Close() { _ = unix.Close(w.fd) }
func (w *serverPIDWatcher) Exited() (bool, error) {
	fds := []unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}
	_, err := unix.Poll(fds, 0)
	if err == unix.EINTR {
		return false, nil
	}
	return fds[0].Revents != 0, err
}

func (w *serverPIDWatcher) Signal(signal syscall.Signal) error {
	return unix.PidfdSendSignal(w.fd, signal, nil, 0)
}
