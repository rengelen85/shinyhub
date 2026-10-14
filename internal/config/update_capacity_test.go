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

func TestUpdateCapacityRejectsInvalidBudgets(t *testing.T) {
	for _, field := range []string{
		"worker_cpu_request_cores: 0", "worker_cpu_request_cores: -1", "worker_cpu_request_cores: .nan", "worker_cpu_request_cores: .inf",
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
