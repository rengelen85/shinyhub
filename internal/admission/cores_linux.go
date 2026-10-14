//go:build linux

package admission

import "os"

var systemCgroups = NewCgroupReader(os.DirFS("/"))

func readCPUQuota() (float64, bool) {
	quota, ok, err := systemCgroups.CPUCapacity()
	return quota, ok && err == nil
}

func readCPUConstraints() (map[string]cpuConstraint, error) {
	return systemCgroups.cpuConstraints()
}
