package process

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestOutputPumpHelper(t *testing.T) {
	for i, arg := range os.Args {
		if arg == "native-output-pump" && len(os.Args) > i+2 {
			size, _ := strconv.ParseInt(os.Args[i+2], 10, 64)
			if err := PumpOutput(os.Args[i+1], size, os.Stdin, os.NewFile(3, "ready")); err != nil {
				os.Exit(2)
			}
			os.Exit(0)
		}
	}
}

func TestOutputPumpOwnsRotatingLogAfterControllerClosesItsHandle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run.log")
	log, err := OpenLogFile(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(dir, "pump")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wrapper, []byte(fmt.Sprintf("#!/bin/sh\nexec %q -test.run=^TestOutputPumpHelper$ -- \"$@\"\n", executable)), 0700); err != nil {
		t.Fatal(err)
	}
	pump, writer, err := startOutputPump(wrapper, log)
	if err != nil {
		t.Fatal(err)
	}
	_ = log.Close() // retired controller drops its own original file handle
	_, err = writer.WriteString("output after retirement\n")
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	pump.finish()
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), "output after retirement") {
		t.Fatalf("output=%q err=%v", data, err)
	}
}
