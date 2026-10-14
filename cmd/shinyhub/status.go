package main

import (
	"fmt"
	"github.com/rvben/shinyhub/internal/cli"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/upgrade"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var statusCmd = &cobra.Command{
	Use: "status", Short: "Inspect the local server and retired reload generations",
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.LoadForMaintenance(serverConfigPath())
		if err != nil {
			return err
		}
		if cfg.Server.PIDFile == "" {
			return fmt.Errorf("status requires server.pid_file")
		}
		raw, err := os.ReadFile(cfg.Server.PIDFile)
		if err != nil {
			return err
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || pid < 1 {
			return fmt.Errorf("invalid server PID file")
		}
		leases := &upgrade.RelayLeases{Dir: filepath.Join(cfg.Storage.AppDataDir, ".reload-relays")}
		retired := leases.Status()
		prose := fmt.Sprintf("Server PID %d; %d retired generation(s), %d preserved session(s)", pid, retired.Count, retired.Sessions)
		if retired.Unknown {
			prose += "; some retired state could not be verified"
		}
		for _, generation := range retired.Generations {
			age := "unknown age"
			if !generation.RetiredAt.IsZero() {
				age = time.Since(generation.RetiredAt).Round(time.Second).String()
			}
			prose += fmt.Sprintf("\n  PID %d: %s, %d session(s)", generation.PID, age, generation.Sessions)
		}
		return cli.RenderAction(cmd, "observed", map[string]any{"pid": pid, "retired": retired}, prose)
	},
}
