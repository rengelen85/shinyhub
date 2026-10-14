//go:build linux

package admission_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/admission"
)

// Run only inside a disposable, private cgroup namespace. The process is PID 1
// and owns its container subtree; this test never modifies host service groups.
func TestLinuxNestedCgroupCapacity(t *testing.T) {
	if os.Getenv("CGROUP_E2E") != "1" {
		t.Skip("requires a disposable Linux container with writable private cgroups")
	}
	if os.Getpid() != 1 {
		t.Fatal("must run as PID 1 in a private container")
	}
	root := "/sys/fs/cgroup"
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	mkdir := func(name string) {
		t.Helper()
		if err := os.Mkdir(filepath.Join(root, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	mkdir("test-supervisor")
	write("test-supervisor/cgroup.procs", fmt.Sprint(os.Getpid()))
	write("cgroup.subtree_control", "+cpu +memory")
	mkdir("service")
	write("service/cpu.max", "50000 100000")
	write("service/memory.max", "268435456")
	write("service/cgroup.subtree_control", "+cpu +memory")
	mkdir("service/_supervisor")
	write("service/_supervisor/cgroup.procs", fmt.Sprint(os.Getpid()))
	cores, _ := admission.Detect(0)
	if cores != 0.5 {
		t.Fatalf("nested service CPU capacity = %v, want 0.5", cores)
	}
	mb, _, ok := admission.DetectMemory(0)
	if !ok || mb != 256 {
		t.Fatalf("nested service memory capacity = %v (%v), want 256 MiB", mb, ok)
	}
	if effective, err := admission.EffectiveCPUCores(8); err != nil || effective != 0.5 {
		t.Fatalf("override escaped service quota: %v %v", effective, err)
	}
	// The worker is a sibling of the supervisor. Ancestor usage must include
	// its memory and CPU, not just this control-plane process's consumption.
	mkdir("service/worker")
	worker := exec.Command("/proc/self/exe", "-test.run=^TestLinuxCgroupWorker$")
	worker.Env = append(os.Environ(), "CGROUP_WORKER=1")
	stdout, err := worker.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	worker.Stderr = os.Stderr
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Process.Kill(); _ = worker.Wait() }()
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if line != "ready\n" {
			t.Fatalf("worker failed: %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker readiness timed out")
	}
	availableMB, err := admission.AvailableMemoryMB()
	if err != nil || availableMB < 0 || availableMB > 160 {
		t.Fatalf("sibling memory escaped ancestor accounting: %d MiB, %v", availableMB, err)
	}
	time.Sleep(time.Second)
	availableCPU, err := admission.AvailableCPUCores(context.Background(), 8, "config")
	if err != nil || availableCPU > 0.2 {
		t.Fatalf("sibling CPU escaped ancestor accounting: %v cores, %v", availableCPU, err)
	}
}

func TestLinuxCgroupWorker(t *testing.T) {
	if os.Getenv("CGROUP_WORKER") != "1" {
		t.Skip("container worker helper")
	}
	if err := os.WriteFile("/sys/fs/cgroup/service/worker/cgroup.procs", []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 128*1024*1024)
	for i := range data {
		data[i] = 1
	}
	fmt.Println("ready")
	for {
		for i := 0; i < len(data); i += 4096 {
			data[i]++
		}
		runtime.KeepAlive(data)
	}
}
