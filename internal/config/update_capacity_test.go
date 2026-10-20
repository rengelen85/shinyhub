package config_test

import (
	"strings"
	"testing"

	"github.com/rvben/shinyhub/internal/config"
)

func TestUpdateCapacityLoadsExplicitFractionalBudgets(t *testing.T) {
	t.Setenv("SHINYHUB_AUTH_SECRET", testSecret)
	cfg, err := config.Load(writeYAML(t, "server:\n  update_capacity:\n    worker_cpu_request_cores: 0.4\n    reserved_cpu_cores: 0.1\n    max_surge_replicas: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	u := cfg.Server.UpdateCapacity
	if u.CPURequest() != 0.4 || u.CPUReserve(8) != 0.1 || u.MaxSurgeReplicas != 1 {
		t.Fatalf("explicit update budget not loaded: %+v", u)
	}
}

func TestUpdateCapacityExplicitZeroDisablesOnlyStaticAccounting(t *testing.T) {
	t.Setenv("SHINYHUB_AUTH_SECRET", testSecret)
	cfg, err := config.Load(writeYAML(t, "server:\n  update_capacity:\n    worker_cpu_request_cores: 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	u := cfg.Server.UpdateCapacity
	if u.CPURequest() != 0 || u.StartupCPURequest() != 0.1 || !u.CPUFits(1, 100, 1) || u.CPUFleetWarning(1, 100) != "" {
		t.Fatalf("explicit zero did not disable static accounting: %+v", u)
	}
}
func TestUpdateCapacityRejectsInvalidBudgets(t *testing.T) {
	for _, field := range []string{
		"worker_cpu_request_cores: -1", "worker_cpu_request_cores: .nan", "worker_cpu_request_cores: .inf",
		"reserved_cpu_cores: -1", "reserved_cpu_cores: .nan", "reserved_cpu_cores: .inf", "max_surge_replicas: -1",
	} {
		t.Run(field, func(t *testing.T) {
			t.Setenv("SHINYHUB_AUTH_SECRET", testSecret)
			_, err := config.Load(writeYAML(t, "server:\n  update_capacity:\n    "+field+"\n"))
			if err == nil || !strings.Contains(err.Error(), "server.update_capacity.") {
				t.Fatalf("invalid budget accepted or unclear error: %v", err)
			}
		})
	}
}

func TestUpdateCapacityZeroStillWarnsAboutImpossibleStartupReserve(t *testing.T) {
	zero, reserve := 0.0, 1.0
	u := config.UpdateCapacityConfig{WorkerCPURequestCores: &zero, ReservedCPUCores: &reserve}
	if warning := u.CPUFleetWarning(1, 100); !strings.Contains(warning, "cannot admit even one startup worker") {
		t.Fatalf("impossible startup reserve did not warn: %q", warning)
	}
}
func TestUpdateCapacityMinimumSurgeWarning(t *testing.T) {
	one := 1.0
	for _, tc := range []struct {
		name   string
		config config.UpdateCapacityConfig
		cores  float64
		warn   bool
	}{
		{"small host defaults", config.UpdateCapacityConfig{}, 1, false},
		{"legacy request", config.UpdateCapacityConfig{WorkerCPURequestCores: &one}, 1, true},
		{"fractional cgroup", config.UpdateCapacityConfig{}, 0.2, true},
		{"large reserve", config.UpdateCapacityConfig{ReservedCPUCores: &one}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warning := tc.config.CPUWarning(tc.cores)
			if (warning != "") != tc.warn {
				t.Fatalf("warning = %q, want warning %v", warning, tc.warn)
			}
		})
	}
	if got := (config.UpdateCapacityConfig{}).CPURequest(); got != 0.1 {
		t.Fatalf("default CPU request = %v", got)
	}
}

func TestUpdateCapacityExactFractionalFit(t *testing.T) {
	request, reserve := 0.1, 0.1
	u := config.UpdateCapacityConfig{WorkerCPURequestCores: &request, ReservedCPUCores: &reserve}
	if !u.CPUFits(0.3, 1, 1) || u.CPUWarning(0.3) != "" {
		t.Fatal("floating-point roundoff refused an exact fit")
	}
	if u.CPUFits(0.299999, 1, 1) || u.CPUWarning(0.299999) == "" {
		t.Fatal("roundoff tolerance admitted an actual shortfall")
	}
}
