package upgrade

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

const supervisorPIDEnv = "_SHINYHUB_SUPERVISOR_PID"

// Supervise keeps systemd's direct child alive across server handoffs. A pidfd
// observes the active server even while its retiring parent remains alive.
// The private PID file is the server's publication fence, not systemd's MAINPID.
func Supervise(ctx context.Context, argv, environment []string, pidFile string, hup <-chan os.Signal, logger *slog.Logger) error {
	if pidFile == "" {
		return fmt.Errorf("--supervise requires server.pid_file")
	}
	probe, err := watchServerPID(os.Getpid())
	if err != nil {
		return err
	}
	probe.Close()
	stalePID, _ := readPID(pidFile)
	command := exec.Command(argv[0], argv[1:]...)
	command.Env = append(environment, supervisorPIDEnv+"="+strconv.Itoa(os.Getpid()))
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start supervised server: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	active := command.Process.Pid
	watcher, err := watchServerPID(active)
	if err != nil {
		_ = command.Process.Kill()
		<-exited
		return err
	}
	defer func() { watcher.Close() }()
	published := false
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	refresh := func() error {
		pid, err := readPID(pidFile)
		if err != nil {
			if published {
				return err
			}
			return nil
		}
		// Ignore the previous service publication, but accept an early handoff
		// even if the first generation retired between polling ticks.
		if !published && pid != command.Process.Pid && pid == stalePID {
			return nil
		}
		published = true
		if pid == active {
			return nil
		}
		next, err := watchServerPID(pid)
		if err != nil {
			return fmt.Errorf("watch successor %d: %w", pid, err)
		}
		watcher.Close()
		watcher = next
		active = pid
		logger.Info("supervisor tracking active server", "pid", active, "supervisor_pid", os.Getpid())
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			_ = watcher.Signal(syscall.SIGTERM)
			if active != command.Process.Pid {
				_ = command.Process.Signal(syscall.SIGTERM)
			}
			// systemd's control-group shutdown signals retired relays too and bounds
			// the complete stop. Wait for the current controller's graceful drain.
			for {
				dead, err := watcher.Exited()
				if err != nil || dead {
					return nil
				}
				<-tick.C
			}
		case _, ok := <-hup:
			if !ok {
				hup = nil
				continue
			}
			_ = watcher.Signal(syscall.SIGHUP)
		case err := <-exited:
			exited = nil
			if refreshErr := refresh(); refreshErr != nil {
				return refreshErr
			}
			if active == command.Process.Pid {
				if err != nil {
					return err
				}
				return fmt.Errorf("active server exited unexpectedly")
			}
		case <-tick.C:
			if err := refresh(); err != nil {
				return fmt.Errorf("supervisor PID publication: %w", err)
			}
			dead, err := watcher.Exited()
			if err != nil {
				return err
			}
			if dead {
				if active == command.Process.Pid && exited != nil {
					if exitErr := <-exited; exitErr != nil {
						return exitErr
					}
				}
				return fmt.Errorf("active server %d exited unexpectedly; requesting service restart", active)
			}
		}
	}
}
