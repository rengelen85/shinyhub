package upgrade

// The listener wire format remains compatible with tableflip, allowing a first
// upgrade from older binaries. Relay-aware successors acknowledge with a
// different readiness byte: an older successor must never mistake control-plane
// retirement for process exit while it cannot account for retained sessions.
import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const inheritedEnv = "TABLEFLIP_HAS_PARENT_7DIU3"
const relayAwareEnv = "_SHINYHUB_RELAY_HANDOFF_V1"
const relayReadyByte byte = 77

type listenerName [3]string

// RepeatableUpgrader can release its successor after its control plane has
// stopped, while the process continues relaying established connections.
// RetireControlPlane must only be called after publishing sealed backend leases.
type RepeatableUpgrader struct {
	mu             sync.Mutex
	timeout        time.Duration
	pidFile        string
	inherited      map[listenerName]*os.File
	used           map[listenerName]*os.File
	parentReady    *os.File
	parentDone     chan struct{}
	parentErr      error
	hasParent      bool
	parentAware    bool
	relayReady     bool
	ready          bool
	inFlight       bool
	upgraded       bool
	successorAware bool
	retirement     *os.File
	exit           chan struct{}
	stopped        chan struct{}
	stopOnce       sync.Once
	exitOnce       sync.Once
	guard          func() (func(), error)
	guardRelease   func()
	beforeReady    func() error
	upgradeFailure func(error)
}

// NewRepeatable uses tableflip's listener descriptors but a negotiated
// control-plane retirement signal. It does not change session lifetime itself.
func NewRepeatable(timeout time.Duration, pidFile string) (*RepeatableUpgrader, error) {
	u := &RepeatableUpgrader{timeout: timeout, pidFile: pidFile,
		inherited: make(map[listenerName]*os.File), used: make(map[listenerName]*os.File),
		parentDone: make(chan struct{}), exit: make(chan struct{}), stopped: make(chan struct{})}
	if u.timeout <= 0 {
		u.timeout = time.Minute
	}
	if os.Getenv(inheritedEnv) == "" {
		close(u.parentDone)
		return u, nil
	}
	u.hasParent = true
	u.parentAware = os.Getenv(relayAwareEnv) == "1"
	u.parentReady = os.NewFile(3, "upgrade-ready")
	rd := os.NewFile(4, "upgrade-parent")
	syscall.CloseOnExec(3)
	syscall.CloseOnExec(4)
	var names [][]string
	if err := gob.NewDecoder(io.LimitReader(rd, 1<<20)).Decode(&names); err != nil {
		u.parentReady.Close()
		rd.Close()
		return nil, fmt.Errorf("decode inherited listeners: %w", err)
	}
	for i, parts := range names {
		fd := 5 + i
		syscall.CloseOnExec(fd)
		file := os.NewFile(uintptr(fd), "upgrade-listener")
		if len(parts) != 3 {
			file.Close()
			continue
		}
		key := listenerName{parts[0], parts[1], parts[2]}
		if previous := u.inherited[key]; previous != nil {
			previous.Close()
		}
		u.inherited[key] = file
	}
	go func() {
		defer rd.Close()
		n, err := io.Copy(io.Discard, rd)
		if n != 0 {
			err = errors.New("unexpected parent retirement data")
		}
		u.mu.Lock()
		u.parentErr = err
		u.mu.Unlock()
		close(u.parentDone)
	}()
	return u, nil
}

func (u *RepeatableUpgrader) Listen(network, addr string) (net.Listener, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ready {
		return nil, errors.New("listeners must precede Ready")
	}
	key := listenerName{"listener", network, addr}
	if file := u.inherited[key]; file != nil {
		ln, err := net.FileListener(file)
		if err != nil {
			return nil, err
		}
		if unixListener, ok := ln.(*net.UnixListener); ok {
			unixListener.SetUnlinkOnClose(false)
		}
		delete(u.inherited, key)
		u.used[key] = file
		return ln, nil
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	if unixListener, ok := ln.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	fileListener, ok := ln.(interface{ File() (*os.File, error) })
	if !ok {
		ln.Close()
		return nil, fmt.Errorf("listener %T cannot be inherited", ln)
	}
	file, err := fileListener.File()
	if err != nil {
		ln.Close()
		return nil, err
	}
	if previous := u.used[key]; previous != nil {
		previous.Close()
	}
	u.used[key] = file
	return ln, nil
}

func (u *RepeatableUpgrader) Ready() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ready {
		return nil
	}
	if u.beforeReady != nil {
		if err := u.beforeReady(); err != nil {
			return err
		}
	}
	if err := u.writePIDFile(); err != nil {
		return err
	}
	for key, file := range u.inherited {
		file.Close()
		delete(u.inherited, key)
	}
	if u.parentReady != nil {
		ready := byte(42)
		if u.parentAware && u.relayReady {
			ready = relayReadyByte
		}
		_, err := u.parentReady.Write([]byte{ready})
		u.parentReady.Close()
		u.parentReady = nil
		if err != nil {
			return err
		}
	}
	u.ready = true
	return nil
}

func (u *RepeatableUpgrader) writePIDFile() error {
	if u.pidFile != "" {
		file, err := os.CreateTemp(filepath.Dir(u.pidFile), ".shinyhub-pid-*")
		if err != nil {
			return err
		}
		defer os.Remove(file.Name())
		_, err = file.WriteString(strconv.Itoa(os.Getpid()) + "\n")
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err := os.Rename(file.Name(), u.pidFile); err != nil {
			return err
		}
	}
	return nil
}

// SetBeforeReady installs a publication step that must succeed before the
// parent receives its readiness acknowledgement.
func (u *RepeatableUpgrader) SetBeforeReady(fn func() error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.beforeReady = fn
}

// SetUpgradeFailure restores owner publication after a killed or crashed child
// that may have published its endpoint but never completed readiness.
func (u *RepeatableUpgrader) SetUpgradeFailure(fn func(error)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.upgradeFailure = fn
}

// EnableRelayHandoff may be called only after the current-owner relay RPC is
// serving and exact backend leases are wired into recovery and lifecycle guards.
func (u *RepeatableUpgrader) EnableRelayHandoff() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.relayReady = true
}

func (u *RepeatableUpgrader) Exit() <-chan struct{} { return u.exit }
func (u *RepeatableUpgrader) HasParent() bool       { return u.hasParent }
func (u *RepeatableUpgrader) WaitForParent(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-u.parentDone:
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.parentErr
}

// upgradeAdmissionLocked checks state before the guard pauses app operations.
// It is repeated after the guard in case shutdown or another upgrade intervenes.
func (u *RepeatableUpgrader) upgradeAdmissionLocked() error {
	select {
	case <-u.stopped:
		return errors.New("terminating")
	default:
	}
	if u.inFlight || u.upgraded {
		return errors.New("upgrade already in progress or completed")
	}
	if !u.ready {
		return &TransitionError{Reason: "upgrade is not ready"}
	}
	select {
	case <-u.parentDone:
	default:
		return &TransitionError{Reason: "parent control plane has not retired"}
	}
	return u.parentErr
}

func (u *RepeatableUpgrader) Upgrade() (result error) {
	u.mu.Lock()
	if err := u.upgradeAdmissionLocked(); err != nil {
		u.mu.Unlock()
		return err
	}
	guard := u.guard
	failure := u.upgradeFailure
	u.mu.Unlock()
	var release func()
	if guard != nil {
		var err error
		release, err = guard()
		if err != nil {
			return err
		}
	}
	keepGuard := false
	defer func() {
		if !keepGuard && release != nil {
			release()
		}
	}()
	u.mu.Lock()
	if err := u.upgradeAdmissionLocked(); err != nil {
		u.mu.Unlock()
		return err
	}
	u.inFlight = true
	files := make([]*os.File, 0, len(u.used))
	names := make([][]string, 0, len(u.used))
	for key, file := range u.used {
		files = append(files, file)
		names = append(names, []string{key[0], key[1], key[2]})
	}
	u.mu.Unlock()
	defer func() { u.mu.Lock(); u.inFlight = false; u.mu.Unlock() }()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer readyR.Close()
	defer readyW.Close()
	namesR, namesW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer namesR.Close()
	keepRetirement := false
	defer func() {
		if !keepRetirement {
			namesW.Close()
		}
	}()
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.ExtraFiles = append([]*os.File{readyW, namesR}, files...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, inheritedEnv+"=") && !strings.HasPrefix(value, relayAwareEnv+"=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Env = append(cmd.Env, inheritedEnv+"=yes", relayAwareEnv+"=1")
	if err := cmd.Start(); err != nil {
		return err
	}
	readyW.Close()
	namesR.Close()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	writeDone := make(chan error, 1)
	go func() { writeDone <- gob.NewEncoder(namesW).Encode(names) }()
	ack := make(chan byte, 1)
	go func() {
		var b [1]byte
		if _, err := io.ReadFull(readyR, b[:]); err == nil {
			ack <- b[0]
		}
	}()
	timer := time.NewTimer(u.timeout)
	defer timer.Stop()
	success := false
	defer func() {
		if !success {
			_ = cmd.Process.Kill()
			if err := u.writePIDFile(); err != nil {
				result = errors.Join(result, fmt.Errorf("restore live PID file: %w", err))
			}
			if failure != nil {
				failure(result)
			}
		}
	}()
	for {
		select {
		case err := <-writeDone:
			if err != nil {
				return fmt.Errorf("send inherited listeners: %w", err)
			}
			writeDone = nil
		case value := <-ack:
			if value != 42 && value != relayReadyByte {
				return fmt.Errorf("invalid successor readiness: %d", value)
			}
			u.mu.Lock()
			u.successorAware = value == relayReadyByte
			u.retirement = namesW
			u.upgraded = true
			u.guardRelease = release
			keepGuard = true
			for key, file := range u.used {
				file.Close()
				delete(u.used, key)
			}
			u.mu.Unlock()
			keepRetirement, success = true, true
			u.exitOnce.Do(func() { close(u.exit) })
			return nil
		case err := <-exited:
			return fmt.Errorf("successor exited before readiness: %v", err)
		case <-timer.C:
			return errors.New("successor readiness timed out")
		case <-u.stopped:
			return errors.New("upgrade interrupted by shutdown")
		}
	}
}

// CheckRelaySuccessor probes a side-effect-free command before starting a
// handoff. A legacy binary cannot safely own a fleet with live older relays.
func CheckRelaySuccessor(ctx context.Context) error {
	command := exec.CommandContext(ctx, os.Args[0], "relay-protocol")
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, inheritedEnv+"=") && !strings.HasPrefix(value, relayAwareEnv+"=") {
			command.Env = append(command.Env, value)
		}
	}
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("successor does not support session relays: %w", err)
	}
	if strings.TrimSpace(string(output)) != "1" {
		return errors.New("unsupported successor relay protocol")
	}
	return nil
}

// SetUpgradeGuard installs a fail-safe admission check before spawning a child.
func (u *RepeatableUpgrader) SetUpgradeGuard(guard func() (func(), error)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.guard = guard
}

// ReleaseUpgradeFence is called only once old lifecycle work has joined and
// relay protection has been sealed (or the process is finally exiting).
func (u *RepeatableUpgrader) ReleaseUpgradeFence() {
	u.mu.Lock()
	release := u.guardRelease
	u.guardRelease = nil
	u.mu.Unlock()
	if release != nil {
		release()
	}
}

// RetireControlPlane releases only a successor that explicitly supports relay
// leases. Legacy successors remain fenced until Stop at actual process exit.
func (u *RepeatableUpgrader) RetireControlPlane() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.upgraded || !u.successorAware {
		return false
	}
	if u.retirement != nil {
		u.retirement.Close()
		u.retirement = nil
	}
	return true
}

func (u *RepeatableUpgrader) SuccessorRelayAware() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.upgraded && u.successorAware
}

func (u *RepeatableUpgrader) Stop() {
	u.stopOnce.Do(func() {
		close(u.stopped)
		u.mu.Lock()
		for _, file := range u.used {
			file.Close()
		}
		for _, file := range u.inherited {
			file.Close()
		}
		if u.parentReady != nil {
			u.parentReady.Close()
		}
		u.mu.Unlock()
		u.exitOnce.Do(func() { close(u.exit) })
	})
}

// A bounded-drain or legacy handoff is signalled by actual process exit, not
// defer ordering. Retain references so os.File finalizers cannot release its
// retirement pipe or lifecycle fence between runServe returning and os.Exit.
var exitResources struct {
	sync.Mutex
	upgraders []*RepeatableUpgrader
}

func (u *RepeatableUpgrader) Close() {
	u.Stop()
	exitResources.Lock()
	exitResources.upgraders = append(exitResources.upgraders, u)
	exitResources.Unlock()
}
