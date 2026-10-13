//go:build !linux

package admission

import "fmt"

func readCPUUsage() (float64, error) {
	return 0, fmt.Errorf("cgroup CPU usage is unavailable on this platform")
}
