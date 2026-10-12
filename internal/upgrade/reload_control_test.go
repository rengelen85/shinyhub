package upgrade

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
)

type reloadTestUpgrader struct {
	*fakeUpgrader
	run func() error
}

func (u *reloadTestUpgrader) Upgrade() error { return u.run() }

func TestAcknowledgedReloadRefusalAndSuccess(t *testing.T) {
	dir, err := os.MkdirTemp("", "sh-reload-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	pidFile := filepath.Join(dir, "pid")
	writePID := func(pid int) {
		t.Helper()
		if err := os.WriteFile(pidFile, []byte(fmt.Sprint(pid)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	writePID(os.Getpid())
	var ready atomic.Bool
	u := &reloadTestUpgrader{fakeUpgrader: newFakeUpgrader(), run: func() error {
		if !ready.Load() {
			return errors.New("dashboard: elastic pool")
		}
		return os.WriteFile(pidFile, []byte(fmt.Sprint(os.Getpid()+1)), 0600)
	}}
	state := &SignalState{}
	stop, err := StartReloadControl(context.Background(), pidFile, time.Second, u, state, func() bool { return true }, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	info, err := os.Stat(reloadSocket(pidFile, os.Getpid()))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions: %v %v", info, err)
	}
	if _, err := RequestReload(context.Background(), pidFile, time.Second, time.Second); err == nil || !strings.Contains(err.Error(), "elastic pool") {
		t.Fatalf("refusal = %v", err)
	}
	if state.SuccessfulHandoff() {
		t.Fatal("refusal recorded as successful handoff")
	}
	successor, err := net.Listen("unix", reloadSocket(pidFile, os.Getpid()+1))
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	var probes atomic.Int32
	go func() {
		for {
			conn, err := successor.Accept()
			if err != nil {
				return
			}
			var request reloadRequest
			_ = json.NewDecoder(conn).Decode(&request)
			n := probes.Add(1)
			_ = json.NewEncoder(conn).Encode(ReloadResult{PID: os.Getpid() + 1, Ready: n >= 2})
			conn.Close()
		}
	}()
	ready.Store(true)
	result, err := RequestReload(context.Background(), pidFile, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.PID != os.Getpid()+1 || !result.Ready || probes.Load() < 2 || !state.SuccessfulHandoff() {
		t.Fatalf("result = %+v", result)
	}
	stop()
	if _, err := os.Stat(reloadSocket(pidFile, os.Getpid())); !os.IsNotExist(err) {
		t.Fatalf("socket survived retirement: %v", err)
	}
}

func TestReloadWaitRetriesOnlyTransitionsAndReportsTimeout(t *testing.T) {
	var calls atomic.Int32
	u := &reloadTestUpgrader{fakeUpgrader: newFakeUpgrader(), run: func() error {
		if calls.Add(1) < 2 {
			return &TransitionError{Reason: "dashboard: draining generation"}
		}
		return nil
	}}
	state := &SignalState{}
	if err := state.Reload(context.Background(), u, time.Second, quietLogger()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("attempts = %d", calls.Load())
	}
	if err := state.Reload(context.Background(), u, time.Second, quietLogger()); err == nil {
		t.Fatal("retiring process accepted another handoff")
	}
	u.run = func() error { return &TransitionError{Reason: "dashboard: draining generation"} }
	state = &SignalState{}
	if err := state.Reload(context.Background(), u, 10*time.Millisecond, quietLogger()); err == nil || !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "dashboard") {
		t.Fatalf("timeout = %v", err)
	}
	if state.SuccessfulHandoff() {
		t.Fatal("timed-out request recorded as handoff")
	}
}

func TestReloadConcurrentRequestsAndCancellation(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	u := &reloadTestUpgrader{fakeUpgrader: newFakeUpgrader(), run: func() error { close(started); <-finish; return errTest }}
	state := &SignalState{}
	result := make(chan error, 1)
	go func() { result <- state.Reload(context.Background(), u, 0, quietLogger()) }()
	<-started
	if err := state.Reload(context.Background(), u, 0, quietLogger()); err == nil {
		t.Fatal("concurrent handoff accepted")
	}
	close(finish)
	<-result
	u.run = func() error { return &TransitionError{Reason: "dashboard: draining generation"} }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.Reload(ctx, u, time.Hour, quietLogger()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
}

func TestReloadControlRejectsPublicDirectoryAndMalformedRequest(t *testing.T) {
	dir, err := os.MkdirTemp("", "sh-reload-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	pidFile := filepath.Join(dir, "pid")
	u := newFakeUpgrader()
	dir = filepath.Join(dir, ".reload")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if stop, err := StartReloadControl(context.Background(), pidFile, time.Second, u, &SignalState{}, func() bool { return true }, quietLogger()); err == nil {
		stop()
		t.Fatal("public directory accepted")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	stop, err := StartReloadControl(context.Background(), pidFile, time.Second, u, &SignalState{}, func() bool { return true }, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	conn, err := net.Dial("unix", reloadSocket(pidFile, os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = fmt.Fprintln(conn, `{"version":2,"wait":0}`)
	var buf [1024]byte
	n, err := conn.Read(buf[:])
	if err != nil || !strings.Contains(string(buf[:n]), "unsupported") {
		t.Fatalf("invalid request = %s %v", buf[:n], err)
	}
	if u.upgradeCount() != 0 {
		t.Fatal("malformed request launched a successor")
	}
}

func TestReloadCallerCancellationReleasesWaitAndAdmission(t *testing.T) {
	dir, err := os.MkdirTemp("", "sh-reload-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	pidFile := filepath.Join(dir, "pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	u := &reloadTestUpgrader{fakeUpgrader: newFakeUpgrader(), run: func() error {
		attempts.Add(1)
		return &TransitionError{Reason: "dashboard: draining generation"}
	}}
	state := &SignalState{}
	stop, err := StartReloadControl(context.Background(), pidFile, time.Second, u, state, func() bool { return true }, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := RequestReload(ctx, pidFile, time.Hour, time.Second); done <- err }()
	waitFor(t, func() bool { return attempts.Load() > 0 }, "waiter reached guard")
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled caller got success")
	}
	waitFor(t, func() bool { state.mu.Lock(); defer state.mu.Unlock(); return state.inFlight == nil }, "waiting reload releases admission")
	before := attempts.Load()
	if err := state.Reload(context.Background(), u, 0, quietLogger()); err == nil {
		t.Fatal("expected current blocker")
	}
	if attempts.Load() != before+1 {
		t.Fatal("new reload could not enter admission after cancellation")
	}
}

func TestReloadControlRecoversStaleSocketWithoutReplacingLiveSocketOrFiles(t *testing.T) {
	dir, err := os.MkdirTemp("", "sh-reload-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	pidFile := filepath.Join(dir, "pid")
	path := reloadSocket(pidFile, os.Getpid())
	if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false)
	old.Close() // Models a crash leaving the PID-named socket on disk.
	start := func() (func(), error) {
		return StartReloadControl(context.Background(), pidFile, time.Second, newFakeUpgrader(), &SignalState{}, func() bool { return true }, quietLogger())
	}
	stop, err := start()
	if err != nil {
		t.Fatalf("stale socket recovery: %v", err)
	}
	defer stop()
	if secondStop, err := start(); err == nil {
		secondStop()
		t.Fatal("replaced a live socket")
	}
	result, err := exchangeReload(context.Background(), path, reloadRequest{Version: 1, Check: true})
	if err != nil || !result.Ready {
		t.Fatalf("original endpoint lost: %+v %v", result, err)
	}
	stop()
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if fileStop, err := start(); err == nil {
		fileStop()
		t.Fatal("replaced a regular file")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "keep" {
		t.Fatalf("existing file changed: %q %v", data, err)
	}
}
