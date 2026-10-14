//go:build !unix

package runtimesocket

import "os"

func ownedByCurrentUser(os.FileInfo) bool { return false }
