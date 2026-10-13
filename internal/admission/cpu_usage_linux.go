//go:build linux

package admission

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func readCPUUsage() (float64, error) {
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.stat"); err == nil {
		fields := strings.Fields(string(b))
		for i := 0; i+1 < len(fields); i += 2 {
			if fields[i] == "usage_usec" {
				n, err := strconv.ParseFloat(fields[i+1], 64)
				if err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
					return 0, fmt.Errorf("invalid cgroup CPU usage")
				}
				return n / 1e6, nil
			}
		}
		return 0, fmt.Errorf("cgroup CPU usage is unavailable")
	}
	b, err := os.ReadFile("/sys/fs/cgroup/cpuacct/cpuacct.usage")
	if err != nil {
		return 0, fmt.Errorf("read cgroup CPU usage: %w", err)
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("invalid cgroup CPU usage")
	}
	return n / 1e9, nil
}
