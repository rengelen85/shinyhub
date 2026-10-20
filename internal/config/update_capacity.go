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
	// Explicit zero disables static worker accounting. The sampled pressure
	// check still budgets the default startup request and the node reserve.
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
	return 0.1
}

func (u UpdateCapacityConfig) StartupCPURequest() float64 {
	if request := u.CPURequest(); request != 0 {
		return request
	}
	return 0.1
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
		{"worker_cpu_request_cores", u.WorkerCPURequestCores, true},
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

// CPUWarning checks the minimum live-update budget: one serving worker and one
// candidate. It is advisory; the running fleet and pressure can require more.

func (u UpdateCapacityConfig) CPUWarning(nodeCores float64) string {
	return u.CPUFleetWarning(nodeCores, 1)
}

// CPUFleetWarning checks one surge candidate against all local live requests.
// With no serving workers yet, check the minimum eventual serving-plus-surge
// pair so an empty startup cannot conceal an unusable policy.
func (u UpdateCapacityConfig) CPUFleetWarning(nodeCores float64, liveWorkers int) string {
	workers := max(1, liveWorkers)
	request, reserve := u.CPURequest(), u.CPUReserve(nodeCores)
	if request == 0 {
		startup := u.StartupCPURequest()
		if FitsCPUBudget(startup+reserve, nodeCores) {
			return ""
		}
		return fmt.Sprintf("server.update_capacity cannot admit even one startup worker: %.2f cores for one candidate plus %.2f reserved exceed %.2f effective cores; lower reserved_cpu_cores or add capacity; live updates will be blocked and scheduled rolls will defer", startup, reserve, nodeCores)
	}
	if u.CPUFits(nodeCores, workers, 1) {
		return ""
	}
	return fmt.Sprintf("server.update_capacity cannot admit even one surge worker alongside %d live worker(s): %.2f cores allocated to live workers plus %.2f for one candidate and %.2f reserved exceed %.2f effective cores; lower worker_cpu_request_cores or reserved_cpu_cores, or add capacity; live updates will be blocked and scheduled rolls will defer", workers, float64(workers)*request, request, reserve, nodeCores)
}

// CPUFits allows negligible floating-point roundoff at an exact budget fit.
// Invalid or overflowing accounting still fails closed.
func (u UpdateCapacityConfig) CPUFits(nodeCores float64, liveWorkers, candidates int) bool {
	if u.CPURequest() == 0 {
		return nodeCores > 0 && !math.IsNaN(nodeCores) && !math.IsInf(nodeCores, 0)
	}
	allocated := float64(liveWorkers)*u.CPURequest() + float64(candidates)*u.CPURequest() + u.CPUReserve(nodeCores)
	if nodeCores <= 0 || math.IsNaN(nodeCores) || math.IsInf(nodeCores, 0) || math.IsNaN(allocated) || math.IsInf(allocated, 0) {
		return false
	}
	return FitsCPUBudget(allocated, nodeCores)
}

// FitsCPUBudget compares accounting or measured headroom without turning a
// few rounding steps into a capacity shortage. It permits four representable
// steps for multiplication and addition, not an extra resource allowance.
// Unknown, invalid and overflowing quantities continue to fail closed.
func FitsCPUBudget(required, available float64) bool {
	if required < 0 || available < 0 || math.IsNaN(required) || math.IsInf(required, 0) || math.IsNaN(available) || math.IsInf(available, 0) {
		return false
	}
	return required <= available || required-available <= 4*(available-math.Nextafter(available, 0))
}
