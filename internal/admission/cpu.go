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
func AvailableCPUCores(ctx context.Context, cores float64, _ string) (float64, error) {
	if cores <= 0 || math.IsNaN(cores) || math.IsInf(cores, 0) {
		return 0, fmt.Errorf("invalid CPU capacity")
	}
	before, err := readCPUConstraints()
	if err != nil {
		return 0, err
	}
	for _, c := range before {
		cores = math.Min(cores, c.cores)
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
	after, err := readCPUConstraints()
	if err != nil {
		return 0, err
	}
	elapsed := time.Since(started).Seconds()
	if len(after) != len(before) || elapsed <= 0 {
		return 0, fmt.Errorf("cgroup CPU constraints changed")
	}
	for path, first := range before {
		last, ok := after[path]
		if !ok || last.cores != first.cores || last.usage < first.usage {
			return 0, fmt.Errorf("cgroup CPU constraints changed or counter reset")
		}
		available = math.Min(available, math.Max(0, first.cores-(last.usage-first.usage)/elapsed))
	}
	return available, nil
}

type cpuConstraint struct{ cores, usage float64 }

// EffectiveCPUCores clamps an accounting override to actual kernel quotas.
// An override can reduce an allocation; it cannot enlarge a service's quota.
func EffectiveCPUCores(cores float64) (float64, error) {
	constraints, err := readCPUConstraints()
	if err != nil {
		return 0, err
	}
	for _, c := range constraints {
		cores = math.Min(cores, c.cores)
	}
	return cores, nil
}
