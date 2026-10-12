package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rvben/shinyhub/internal/config"
)

func TestLoad_DrainTimeoutDefault(t *testing.T) {
	t.Setenv("SHINYHUB_AUTH_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.DrainTimeout != 60*time.Second {
		t.Fatalf("DrainTimeout default = %v, want 60s", cfg.Server.DrainTimeout)
	}
}

func TestLoad_DrainTimeoutExplicit(t *testing.T) {
	t.Setenv("SHINYHUB_AUTH_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  drain_timeout: 5m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.DrainTimeout != 5*time.Minute {
		t.Fatalf("DrainTimeout = %v, want 5m", cfg.Server.DrainTimeout)
	}
}

func TestLoad_ReloadDrainTimeout(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		want       time.Duration
		invalid    bool
	}{
		{"default", "server: {}", time.Minute, false},
		{"inherit", "server:\n  drain_timeout: 10m", 10 * time.Minute, false},
		{"separate", "server:\n  drain_timeout: 10m\n  reload_drain_timeout: 8h", 8 * time.Hour, false},
		{"zero", "server:\n  reload_drain_timeout: 0s", 0, true},
		{"negative", "server:\n  reload_drain_timeout: -1s", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SHINYHUB_AUTH_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(path)
			if tc.invalid {
				if err == nil {
					t.Fatal("expected invalid reload timeout to fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.Server.EffectiveReloadDrainTimeout(); got != tc.want {
				t.Fatalf("reload drain=%v, want %v", got, tc.want)
			}
			if tc.name == "separate" && cfg.Server.DrainTimeout != 10*time.Minute {
				t.Fatal("reload timeout changed app drain timeout")
			}
		})
	}
}

func TestLoad_ReloadDrainTimeoutEnv(t *testing.T) {
	for _, value := range []string{"2h", "0s", "-1s", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("SHINYHUB_AUTH_SECRET", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
			t.Setenv("SHINYHUB_RELOAD_DRAIN_TIMEOUT", value)
			cfg, err := config.Load("")
			if value != "2h" {
				if err == nil {
					t.Fatal("expected invalid environment override to fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Server.EffectiveReloadDrainTimeout() != 2*time.Hour || cfg.Server.DrainTimeout != time.Minute {
				t.Fatal("reload override must leave app timeout unchanged")
			}
		})
	}
}
