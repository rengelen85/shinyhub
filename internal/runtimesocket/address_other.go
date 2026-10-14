//go:build !linux

package runtimesocket

func unixAddress(path string) (string, func(), error) {
	return path, func() {}, nil
}
