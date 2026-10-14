//go:build linux || darwin

package localtoken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rvben/shinyhub/internal/auth"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Unix socket paths must fit even on macOS, whose testing temp paths are long.
func runtimePath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sht-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "server.pid")
}

func TestControlAuthenticatesActualPeer(t *testing.T) {
	pidFile := runtimePath(t)
	if err := os.WriteFile(pidFile, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	stop, err := Start(context.Background(), pidFile, func(_ context.Context, scope string, ttl time.Duration) (string, error) {
		calls.Add(1)
		token, _, _, err := auth.IssueLocalToken("socket-test-authentication-secret-32-bytes", scope, ttl)
		return token, err
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socket(pidFile, os.Getpid()), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	_ = json.NewEncoder(c).Encode(request{Version: 1, Scope: "announcements", TTL: time.Minute})
	var out response
	err = json.NewDecoder(c).Decode(&out)
	if os.Geteuid() != 0 {
		if err == nil || calls.Load() != 0 {
			t.Fatal("non-root peer obtained capability")
		}
		if _, err := Request(context.Background(), pidFile, "announcements", time.Minute); err == nil {
			t.Fatal("non-root client accepted")
		}
	} else {
		if err != nil || !strings.HasPrefix(out.Token, auth.LocalTokenPrefix) || calls.Load() != 1 {
			t.Fatalf("root issuance failed: %v %+v", err, out)
		}
		token, err := Request(context.Background(), pidFile, "announcements", time.Minute)
		if err != nil || !strings.HasPrefix(token, auth.LocalTokenPrefix) {
			t.Fatalf("client failed: %v", err)
		}
		req := httptest.NewRequest("GET", "/api/announcements", nil)
		req.Header.Set("Authorization", "Token "+token)
		user, _, err := auth.AuthenticateRequest(req, "socket-test-authentication-secret-32-bytes", nil, nil, nil)
		if err != nil || user == nil || !user.LocalAnnouncements {
			t.Fatalf("minted token cannot authenticate: %v", err)
		}
		if token == out.Token {
			t.Fatal("reused token identity across issuance requests")
		}

	}
	stop()
	stop()
	if _, err := os.Lstat(socket(pidFile, os.Getpid())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket not removed: %v", err)
	}
}

func TestControlRefusesUnsafeRuntimeDirectory(t *testing.T) {
	pidFile := runtimePath(t)
	dir := filepath.Dir(socket(pidFile, os.Getpid()))
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if stop, err := Start(context.Background(), pidFile, func(context.Context, string, time.Duration) (string, error) { return "", nil }); err == nil {
		stop()
		t.Fatal("accepted public runtime directory")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(filepath.Dir(pidFile), "target")
	if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, socket(pidFile, os.Getpid())); err != nil {
		t.Fatal(err)
	}
	if stop, err := Start(context.Background(), pidFile, func(context.Context, string, time.Duration) (string, error) { return "", nil }); err == nil {
		stop()
		t.Fatal("replaced pre-existing endpoint")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "keep" {
		t.Fatal("damaged pre-existing file")
	}
}

func TestControlRejectsInvalidRequestsAndIssuanceFailure(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-only protocol test; non-root denial tested separately")
	}
	pidFile := runtimePath(t)
	var calls atomic.Int32
	stop, err := Start(context.Background(), pidFile, func(context.Context, string, time.Duration) (string, error) {
		calls.Add(1)
		return "", errors.New("secret internal error")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	for _, body := range []string{
		`{"version":2,"scope":"announcements","ttl":60000000000}`,
		`{"version":1,"scope":"admin","ttl":60000000000}`,
		`{"version":1,"scope":"announcements","ttl":0}`,
		`{"version":1,"scope":"announcements","ttl":960000000000}`,
		`{"version":1,"scope":"announcements","ttl":60000000000,"uid":0}`,
		`{`,
		`{"version":1,"scope":"announcements","ttl":60000000000}`,
	} {
		c, err := net.Dial("unix", socket(pidFile, os.Getpid()))
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(time.Second))
		_, _ = fmt.Fprintln(c, body)
		// Finish the malformed request without waiting for the read deadline.
		_ = c.(*net.UnixConn).CloseWrite()
		var out response
		err = json.NewDecoder(c).Decode(&out)
		c.Close()
		if err != nil || out.Token != "" || out.Error == "" || out.Error == "secret internal error" {
			t.Fatalf("unsafe response: %+v %v", out, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("issuer called for invalid input: %d", calls.Load())
	}
}

func TestStopCancelsInFlightIssuance(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-only issuance cancellation test")
	}
	pidFile := runtimePath(t)
	entered := make(chan struct{})
	stop, err := Start(context.Background(), pidFile, func(ctx context.Context, _ string, _ time.Duration) (string, error) {
		close(entered)
		<-ctx.Done()
		return "", ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	c, err := net.Dial("unix", socket(pidFile, os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = json.NewEncoder(c).Encode(request{1, "announcements", time.Minute})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("issuer did not start")
	}
	finished := make(chan struct{})
	go func() { stop(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel issuance")
	}
}
