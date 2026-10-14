//go:build linux

package localtoken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/runtimesocket"
)

func TestLongPrivateRuntimeTokenRequestAuthenticatesPeer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("private-", 20))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(dir, "server.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	stop, err := Start(context.Background(), pidFile, func(context.Context, string, time.Duration) (string, error) {
		calls.Add(1)
		return "long-path-token", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	token, err := Request(context.Background(), pidFile, "announcements", time.Minute)
	if os.Geteuid() == 0 {
		if err != nil || token != "long-path-token" || calls.Load() != 1 {
			t.Fatalf("root issuance over long path: %q %v (%d calls)", token, err, calls.Load())
		}
	} else {
		if err == nil || err.Error() != "local-token requires root on the server host" || calls.Load() != 0 {
			t.Fatalf("non-root client refusal: %v (%d calls)", err, calls.Load())
		}
		// Bypass the client-side root guard to exercise kernel peer rejection
		// after successfully dialing the actual long private socket path.
		c, err := runtimesocket.DialContext(context.Background(), socket(pidFile, os.Getpid()))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		_ = json.NewEncoder(c).Encode(request{Version: 1, Scope: "announcements", TTL: time.Minute})
		var out response
		decodeErr := json.NewDecoder(c).Decode(&out)
		var netErr net.Error
		if errors.As(decodeErr, &netErr) && netErr.Timeout() {
			t.Fatal("server did not promptly refuse the non-root peer")
		}
		if out.Token != "" || calls.Load() != 0 {
			t.Fatal("non-root socket peer obtained a token")
		}
	}
	stop()
	if _, err := os.Lstat(socket(pidFile, os.Getpid())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired endpoint not removed: %v", err)
	}
}
