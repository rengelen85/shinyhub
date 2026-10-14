package admission

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path"
	"strconv"
	"strings"
)

// cgroupDirs resolves this process's controller mount and walks its visible
// ancestry. Membership and mount roots differ inside cgroup namespaces, so
// neither /sys/fs/cgroup nor a path from /proc alone is sufficient.
func (r *CgroupReader) dirs(controller string) ([]string, bool, error) {
	if r.files == nil {
		return nil, false, fmt.Errorf("cgroup filesystem is unavailable")
	}
	membership, err := fs.ReadFile(r.files, "proc/self/cgroup")
	if err != nil {
		return nil, false, err
	}
	mounts, err := fs.ReadFile(r.files, "proc/self/mountinfo")
	if err != nil {
		return nil, false, err
	}
	var rel, unifiedRel string
	for _, line := range strings.Split(string(membership), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[0] == "0" && fields[1] == "" {
			unifiedRel = fields[2]
		}
		if containsController(fields[1], controller) {
			rel = fields[2]
		}
	}
	// On hybrid hosts a controller attached to v1 cannot also be attached to
	// v2. Prefer its explicit membership regardless of line order.
	unified := rel == "" && unifiedRel != ""
	if unified {
		rel = unifiedRel
	}
	if rel == "" {
		return nil, unified, nil
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(string(mounts), "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		pre, post := strings.Fields(left), strings.Fields(right)
		if len(pre) < 5 || len(post) < 3 {
			continue
		}
		if unified {
			if post[0] != "cgroup2" {
				continue
			}
		} else if post[0] != "cgroup" || !containsController(post[2], controller) {
			continue
		}
		mount, root := unescape.Replace(pre[4]), unescape.Replace(pre[3])
		suffix := path.Clean(rel)
		if root != "/" && (suffix == root || strings.HasPrefix(suffix, root+"/")) {
			suffix = strings.TrimPrefix(suffix, root)
		}
		current := path.Join(mount, suffix)
		if _, err := fs.Stat(r.files, strings.TrimPrefix(current, "/")); err != nil {
			continue
		}
		var dirs []string
		for dir := current; ; dir = path.Dir(dir) {
			dirs = append(dirs, dir)
			if dir == mount {
				return dirs, unified, nil
			}
			if path.Dir(dir) == dir {
				break
			}
		}
	}
	return nil, unified, fmt.Errorf("cgroup %s mount could not be resolved", controller)
}

func containsController(list, controller string) bool {
	for _, field := range strings.Split(list, ",") {
		if field == controller {
			return true
		}
	}
	return false
}

func (r *CgroupReader) optionalFile(dir, name string) (string, bool, error) {
	b, err := fs.ReadFile(r.files, strings.TrimPrefix(path.Join(dir, name), "/"))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(string(b)), true, nil
}

func (r *CgroupReader) uintFile(dir, name string) (uint64, error) {
	b, err := fs.ReadFile(r.files, strings.TrimPrefix(path.Join(dir, name), "/"))
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return v, nil
}

// CgroupReader reads process membership, mounts and controller files from a
// Linux filesystem. The filesystem root must expose proc/ and the mount paths
// named by proc/self/mountinfo. Injecting that OS boundary permits reproducible
// namespace and hierarchy tests without global environment overrides.
type CgroupReader struct{ files fs.FS }

func NewCgroupReader(files fs.FS) *CgroupReader { return &CgroupReader{files: files} }

func (r *CgroupReader) CPUCapacity() (float64, bool, error) {
	constraints, err := r.cpuConstraints()
	if err != nil {
		return 0, false, err
	}
	if len(constraints) == 0 {
		return 0, false, nil
	}
	quota := math.Inf(1)
	for _, c := range constraints {
		quota = math.Min(quota, c.cores)
	}
	return quota, true, nil
}

func (r *CgroupReader) MemoryLimit() (uint64, bool, error) {
	limits, err := r.memoryConstraints(false)
	if err != nil {
		return 0, false, err
	}
	if len(limits) == 0 {
		return 0, false, nil
	}
	minimum := uint64(math.MaxUint64)
	for _, c := range limits {
		minimum = min(minimum, c.limit)
	}
	return minimum, true, nil
}

const unlimitedMemoryFloor = uint64(1) << 50

// Each limiting ancestor has its own usage counter, including sibling workers.
// Reading only the supervisor's counter understates the service's consumption.
func (r *CgroupReader) cpuConstraints() (map[string]cpuConstraint, error) {
	dirs, unified, err := r.dirs("cpu")
	if err != nil {
		return nil, err
	}
	result := make(map[string]cpuConstraint)
	for _, dir := range dirs {
		name := "cpu.max"
		if !unified {
			name = "cpu.cfs_quota_us"
		}
		raw, exists, err := r.optionalFile(dir, name)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		fields := strings.Fields(raw)
		var quota, period uint64
		if unified {
			if len(fields) != 2 {
				return nil, fmt.Errorf("invalid cpu.max")
			}
			if fields[0] == "max" {
				continue
			}
			quota, err = strconv.ParseUint(fields[0], 10, 64)
			if err == nil {
				period, err = strconv.ParseUint(fields[1], 10, 64)
			}
		} else {
			if raw == "-1" {
				continue
			}
			quota, err = strconv.ParseUint(raw, 10, 64)
			if err == nil {
				period, err = r.uintFile(dir, "cpu.cfs_period_us")
			}
		}
		if err != nil || quota == 0 || period == 0 {
			return nil, fmt.Errorf("invalid cgroup CPU quota in %s", dir)
		}
		usage, err := r.cpuUsageAt(dir, unified)
		if err != nil {
			return nil, err
		}
		result[dir] = cpuConstraint{cores: float64(quota) / float64(period), usage: usage}
	}
	return result, nil
}
func (r *CgroupReader) cpuUsageAt(dir string, unified bool) (float64, error) {
	if unified {
		b, err := fs.ReadFile(r.files, strings.TrimPrefix(path.Join(dir, "cpu.stat"), "/"))
		if err != nil {
			return 0, err
		}
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "usage_usec" {
				n, err := strconv.ParseUint(fields[1], 10, 64)
				if err != nil {
					return 0, fmt.Errorf("invalid cgroup CPU usage")
				}
				return float64(n) / 1e6, nil
			}
		}
		return 0, fmt.Errorf("cgroup CPU usage is unavailable")
	}
	// cpuacct may be mounted separately from cpu on v1. Match the ancestor's
	// depth in that hierarchy rather than assuming a combined controller mount.
	dirs, _, err := r.dirs("cpuacct")
	if err != nil {
		return 0, err
	}
	cpuDirs, _, err := r.dirs("cpu")
	if err != nil {
		return 0, err
	}
	for i, cpuDir := range cpuDirs {
		if cpuDir == dir && i < len(dirs) {
			n, err := r.uintFile(dirs[i], "cpuacct.usage")
			return float64(n) / 1e9, err
		}
	}
	return 0, fmt.Errorf("cgroup CPU accounting ancestor is unavailable")
}

type memoryConstraint struct{ limit, used uint64 }

func (r *CgroupReader) memoryConstraints(withUsage bool) ([]memoryConstraint, error) {
	dirs, unified, err := r.dirs("memory")
	if err != nil {
		return nil, err
	}
	var result []memoryConstraint
	for _, dir := range dirs {
		limitFile, usageFile := "memory.max", "memory.current"
		if !unified {
			limitFile, usageFile = "memory.limit_in_bytes", "memory.usage_in_bytes"
		}
		raw, exists, err := r.optionalFile(dir, limitFile)
		if err != nil {
			return nil, err
		}
		if !exists || raw == "max" {
			continue
		}
		limit, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid cgroup memory limit in %s: %w", dir, err)
		}
		if limit >= unlimitedMemoryFloor {
			continue
		}
		c := memoryConstraint{limit: limit}
		if withUsage {
			c.used, err = r.uintFile(dir, usageFile)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, c)
	}
	return result, nil
}

func (r *CgroupReader) MemoryHeadroom(hostAvailable uint64) (uint64, error) {
	constraints, err := r.memoryConstraints(true)
	if err != nil {
		return 0, err
	}
	for _, c := range constraints {
		remaining := uint64(0)
		if c.limit > c.used {
			remaining = c.limit - c.used
		}
		hostAvailable = min(hostAvailable, remaining)
	}
	return hostAvailable, nil
}
