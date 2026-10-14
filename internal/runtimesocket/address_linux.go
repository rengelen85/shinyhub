//go:build linux

package runtimesocket

import (
	"fmt"
	"os"
	"path/filepath"
)

func unixAddress(path string) (string, func(), error) {
	if len(path) < 108 {
		return path, func() {}, nil
	}
	// Keep the socket in its original private directory. Linux resolves this
	// short descriptor-relative pathname to the same filesystem object, without
	// moving control sockets into a shared directory or an abstract namespace.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return "", nil, err
	}
	address := fmt.Sprintf("/proc/self/fd/%d/%s", dir.Fd(), filepath.Base(path))
	return address, func() { _ = dir.Close() }, nil
}
