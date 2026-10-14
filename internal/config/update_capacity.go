package config

import (
	"fmt"
	"math"
)

// UpdateCapacityConfig controls admission for parallel serving replacements.
// Requests are accounting budgets, distinct from runtime-enforced ceilings.
// Planned unavailability is zero: existing workers are retained until healthy
// candidates are ready. Stop-first replacement requires explicit authorization.
type UpdateCapacityConfig struct {
	WorkerCPURequestCores *float64 `yaml:"worker_cpu_request_cores"`
	ReservedCPUCores      *float64 `yaml:"reserved_cpu_cores"`
	// Zero permits the complete candidate pool; positive values bound extra
	// replicas per app. Version handoffs remain atomic and never partially roll.
	MaxSurgeReplicas int `yaml:"max_surge_replicas"`
}

func (u UpdateCapacityConfig) CPURequest() float64 {
	if u.WorkerCPURequestCores != nil {
		return *u.WorkerCPURequestCores
	}
	return 1
}

func (u UpdateCapacityConfig) CPUReserve(nodeCores float64) float64 {
	if u.ReservedCPUCores != nil {
		return *u.ReservedCPUCores
	}
	return nodeCores * 0.1
}

func (u UpdateCapacityConfig) Validate() error {
	for _, field := range []struct {
		name      string
		value     *float64
		allowZero bool
	}{
		{"worker_cpu_request_cores", u.WorkerCPURequestCores, false},
		{"reserved_cpu_cores", u.ReservedCPUCores, true},
	} {
		if field.value == nil {
			continue
		}
		v := *field.value
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || (v == 0 && !field.allowZero) {
			requirement := "positive"
			if field.allowZero {
				requirement = "non-negative"
			}
			return fmt.Errorf("server.update_capacity.%s must be finite and %s", field.name, requirement)
		}
	}
	if u.MaxSurgeReplicas < 0 {
		return fmt.Errorf("server.update_capacity.max_surge_replicas must be non-negative")
	}
	return nil
}
