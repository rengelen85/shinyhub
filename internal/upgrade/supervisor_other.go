//go:build !linux

package upgrade

import (
	"errors"
	"syscall"
)

type serverPIDWatcher struct{}

func watchServerPID(int) (*serverPIDWatcher, error) {
	return nil, errors.New("--supervise requires Linux with pidfd support")
}
func (*serverPIDWatcher) Close()                {}
func (*serverPIDWatcher) Exited() (bool, error) { return false, errors.New("pidfd unavailable") }

func (*serverPIDWatcher) Signal(syscall.Signal) error { return errors.New("pidfd unavailable") }
