package admission

import (
	"context"
	"fmt"
	"math"
	"time"

	gopscpu "github.com/shirou/gopsutil/v4/cpu"
)

// AvailableCPUCores measures current headroom over a bounded interval. The
// effective node budget is reduced by host pressure; a binding cgroup quota
// additionally budgets the actual CPU time consumed within that cgroup.
// Measurements that cannot prove headroom return an error, never idle capacity.
func AvailableCPUCores(ctx context.Context, cores float64, source string) (float64, error) {
	if cores <= 0 || math.IsNaN(cores) || math.IsInf(cores, 0) {
		return 0, fmt.Errorf("invalid CPU capacity")
	}
	var before float64
	var err error
	if source == "cgroup-quota" {
		before, err = readCPUUsage()
		if err != nil {
			return 0, err
		}
	}
	started := time.Now()
	pcts, err := gopscpu.PercentWithContext(ctx, 200*time.Millisecond, false)
	if err != nil {
		return 0, err
	}
	if len(pcts) != 1 || math.IsNaN(pcts[0]) || math.IsInf(pcts[0], 0) || pcts[0] < 0 || pcts[0] > 100 {
		return 0, fmt.Errorf("host CPU reading is unavailable or invalid")
	}
	available := cores * (1 - pcts[0]/100)
	if source == "cgroup-quota" {
		after, err := readCPUUsage()
		if err != nil {
			return 0, err
		}
		elapsed := time.Since(started).Seconds()
		if after < before || elapsed <= 0 {
			return 0, fmt.Errorf("cgroup CPU counter reset")
		}
		available = math.Min(available, math.Max(0, cores-(after-before)/elapsed))
	}
	return available, nil
}
