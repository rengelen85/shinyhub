// Package localtoken exposes root-only capability issuance on a private Unix socket.
package localtoken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rvben/shinyhub/internal/auth"
	"github.com/rvben/shinyhub/internal/runtimesocket"
)

type request struct {
	Version int           `json:"version"`
	Scope   string        `json:"scope"`
	TTL     time.Duration `json:"ttl"`
}
type response struct {
	Token string `json:"token,omitempty"`
	Error string `json:"error,omitempty"`
}
type Issuer func(context.Context, string, time.Duration) (string, error)

func socket(pidFile string, pid int) string {
	return filepath.Join(filepath.Dir(pidFile), ".reload", fmt.Sprintf("t%d.sock", pid))
}

func Start(ctx context.Context, pidFile string, issue Issuer) (func(), error) {
	if pidFile == "" {
		return func() {}, nil
	}
	if !peerSupported {
		return func() {}, nil
	}
	if issue == nil {
		return nil, errors.New("local-token issuer is required")
	}
	listener, err := runtimesocket.Listen(socket(pidFile, os.Getpid()))
	if err != nil {
		return nil, err
	}
	controlCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := runtimesocket.Accept(listener, slog.Default())
			if err != nil {
				return
			}
			// Verify before reading attacker-controlled data or spawning a handler.
			uid, err := peerUID(c)
			if err != nil || uid != 0 {
				c.Close()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				var req request
				out := response{}
				decoder := json.NewDecoder(io.LimitReader(c, 4096))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&req); err != nil || req.Version != 1 || req.Scope != auth.LocalTokenScope || req.TTL < time.Second || req.TTL > auth.LocalTokenMaxTTL {
					out.Error = "invalid request: scope must be announcements and ttl must be 1s..15m"
				} else if controlCtx.Err() != nil {
					out.Error = "server shutting down"
				} else {
					requestCtx, requestCancel := context.WithTimeout(controlCtx, 5*time.Second)
					defer requestCancel()
					token, err := issue(requestCtx, req.Scope, req.TTL)
					if err != nil {
						slog.Error("host-local token issuance failed", "err", err)
						out.Error = "local token issuance failed; check server logs"
					} else {
						out.Token = token
					}
				}
				_ = json.NewEncoder(c).Encode(out)
			}()
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); listener.Close(); <-done; wg.Wait() }) }, nil
}

func Request(ctx context.Context, pidFile, scope string, ttl time.Duration) (string, error) {
	if !peerSupported {
		return "", errors.New("local-token requires Linux or macOS peer credentials")
	}
	if os.Geteuid() != 0 {
		return "", errors.New("local-token requires root on the server host")
	}
	if pidFile == "" {
		return "", errors.New("local-token requires server.pid_file")
	}
	if scope != auth.LocalTokenScope || ttl < time.Second || ttl > auth.LocalTokenMaxTTL {
		return "", errors.New("scope must be announcements and ttl must be 1s..15m")
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return "", err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return "", errors.New("invalid server.pid_file")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := runtimesocket.DialContext(ctx, socket(pidFile, pid))
	if err != nil {
		return "", fmt.Errorf("local-token socket unavailable: %w", err)
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	_ = c.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	if err := json.NewEncoder(c).Encode(request{1, scope, ttl}); err != nil {
		return "", err
	}
	var out response
	if err := json.NewDecoder(io.LimitReader(c, 8192)).Decode(&out); err != nil {
		return "", err
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	if out.Token == "" {
		return "", errors.New("empty local-token response")
	}
	return out.Token, nil
}
