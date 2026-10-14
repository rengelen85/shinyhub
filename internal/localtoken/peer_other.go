//go:build !linux && !darwin

package localtoken

import (
	"errors"
	"net"
)

const peerSupported = false

func peerUID(*net.UnixConn) (uint32, error) { return 0, errors.New("peer credentials unsupported") }
