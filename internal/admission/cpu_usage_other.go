//go:build !linux

package admission

func readCPUConstraints() (map[string]cpuConstraint, error) { return nil, nil }
