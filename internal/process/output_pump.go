package process

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// PumpOutput is the small re-exec worker owning native stdout across controller
// retirement. It keeps the existing rotating LogFile format and size limits.
func PumpOutput(path string, maxSize int64, input io.Reader, ready *os.File) error {
	log, err := OpenLogFile(path, maxSize)
	if err != nil {
		return err
	}
	defer log.Close()
	if ready != nil {
		if _, err := ready.Write([]byte{1}); err != nil {
			return err
		}
		_ = ready.Close()
	}
	_, err = io.Copy(log, input)
	return err
}

type outputPump struct {
	command *exec.Cmd
	done    chan error
}

func startOutputPump(binary string, log *LogFile) (*outputPump, *os.File, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		writer.Close()
		return nil, nil, err
	}
	defer readyR.Close()
	defer readyW.Close()
	command := exec.Command(binary, "native-output-pump", log.path, strconv.FormatInt(log.maxSize, 10))
	command.Stdin = reader
	command.Stderr = os.Stderr
	command.Env = []string{"PATH=/usr/bin:/bin"}
	command.ExtraFiles = []*os.File{readyW}
	if err := command.Start(); err != nil {
		writer.Close()
		return nil, nil, err
	}
	pump := &outputPump{command: command, done: make(chan error, 1)}
	go func() { pump.done <- command.Wait() }()
	_ = readyW.Close()
	// A deadline on Go-created pipe descriptors bounds a broken/mismatched helper.
	_ = readyR.SetReadDeadline(time.Now().Add(10 * time.Second))
	var ack [1]byte
	if _, err := io.ReadFull(readyR, ack[:]); err != nil || ack[0] != 1 {
		writer.Close()
		_ = command.Process.Kill()
		<-pump.done
		return nil, nil, fmt.Errorf("native output pump did not acknowledge readiness")
	}
	return pump, writer, nil
}

func (pump *outputPump) finish() {
	select {
	case <-pump.done:
	case <-time.After(leaderExitPipeGrace):
		_ = pump.command.Process.Kill()
		<-pump.done
	}
}
