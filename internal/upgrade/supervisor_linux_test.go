//go:build linux

package upgrade

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorHelper(t *testing.T) {
	role := os.Getenv("SUPERVISOR_TEST_ROLE")
	if role == "" {
		return
	}
	pidFile := os.Getenv("SUPERVISOR_TEST_ROLE_FILE")
	publish := func() {
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
	}
	publish()
	if role == "exit7" {
		os.Exit(7)
	}
	if role == "parent" {
		for {
			if _, err := os.Stat(pidFile + ".handoff"); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		child := exec.Command(os.Args[0], "-test.run=^TestSupervisorHelper$")
		for _, e := range os.Environ() {
			if !strings.HasPrefix(e, "SUPERVISOR_TEST_ROLE=") {
				child.Env = append(child.Env, e)
			}
		}
		child.Env = append(child.Env, "SUPERVISOR_TEST_ROLE=successor")
		if err := child.Start(); err != nil {
			os.Exit(3)
		}
		// Deliberately remain alive after the successor exits, reproducing a
		// non-reapable MainPID while older websocket relays keep the cgroup alive.
		go func() { _ = child.Wait() }()
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestSupervisorDetectsCrashAfterHandoffWithRetiredParentAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "server.pid")
	env := append(os.Environ(), "SUPERVISOR_TEST_ROLE=parent", "SUPERVISOR_TEST_ROLE_FILE="+path)
	result := make(chan error, 1)
	go func() {
		result <- Supervise(ctx, []string{os.Args[0], "-test.run=^TestSupervisorHelper$"}, env, path, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	read := func(exclude int) int {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			pid, _ := readPID(path)
			if pid > 0 && pid != exclude {
				return pid
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("PID publication timed out")
		return 0
	}
	parent := read(0)
	defer syscall.Kill(parent, syscall.SIGKILL)
	// Let the supervisor observe the cold-start publication before handoff.
	time.Sleep(150 * time.Millisecond)
	if err := os.WriteFile(path+".handoff", nil, 0600); err != nil {
		t.Fatal(err)
	}
	successor := read(parent)
	defer syscall.Kill(successor, syscall.SIGKILL)
	time.Sleep(150 * time.Millisecond)
	if err := syscall.Kill(successor, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("active server %d exited", successor)) {
			t.Fatalf("supervisor result=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("successor crash was not detected")
	}
	if err := syscall.Kill(parent, 0); err != nil {
		t.Fatalf("test did not retain predecessor: %v", err)
	}
}

func TestSupervisorPreservesPermanentStartupExitCode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pid")
	env := append(os.Environ(), "SUPERVISOR_TEST_ROLE=exit7", "SUPERVISOR_TEST_ROLE_FILE="+path)
	err := Supervise(context.Background(), []string{os.Args[0], "-test.run=^TestSupervisorHelper$"}, env, path, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 7 {
		t.Fatalf("permanent startup error=%v", err)
	}
}
