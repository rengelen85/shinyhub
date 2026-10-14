//go:build linux

package upgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAcknowledgedReloadInLongPrivateRuntimeDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("runtime-", 20))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(dir, "pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	u := &reloadTestUpgrader{fakeUpgrader: newFakeUpgrader(), run: func() error {
		return errors.New("fixture: preserve current sessions")
	}}
	start := func() func() {
		t.Helper()
		stop, err := StartReloadControl(context.Background(), pidFile, time.Second, u, &SignalState{}, func() bool { return true }, quietLogger())
		if err != nil {
			t.Fatalf("valid long runtime directory prevents acknowledged reload: %v", err)
		}
		return stop
	}
	stop := start()
	t.Cleanup(stop)
	if _, err := RequestReload(context.Background(), pidFile, 0, time.Second); err == nil || !strings.Contains(err.Error(), "fixture: preserve current sessions") {
		t.Fatalf("request did not reach the live controller: %v", err)
	}
	if duplicate, err := StartReloadControl(context.Background(), pidFile, time.Second, u, &SignalState{}, func() bool { return true }, quietLogger()); err == nil {
		duplicate()
		t.Fatal("a second controller replaced the live socket")
	}
	sockets, err := filepath.Glob(filepath.Join(dir, ".reload", "*.sock"))
	if err != nil || len(sockets) != 1 {
		t.Fatalf("private socket files = %v, error = %v", sockets, err)
	}
	info, err := os.Stat(sockets[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket must remain private: %v %v", info, err)
	}
	stop()
	if _, err := os.Stat(sockets[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired controller left a socket behind: %v", err)
	}
	stopAgain := start()
	t.Cleanup(stopAgain)
}
