package main

import (
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/rvben/shinyhub/internal/cli"
	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/upgrade"
	"github.com/spf13/cobra"
)

var reloadWait time.Duration

var reloadCmd = &cobra.Command{
	Use:   "reload",
	Short: "Reload the local server and wait for its handoff result",
	Long: "Reloads the local server through its private runtime socket. Requires\n" +
		"server.pid_file with a private .reload directory beside it. Exits nonzero if the\n" +
		"server refuses the reload or the successor fails to become ready.\n" +
		"--wait allows temporary app transitions to finish before handoff;\n" +
		"permanent blockers fail immediately. After handoff, readiness has a separate\n" +
		"server.upgrade_timeout + 1m budget (2m by default), independent of --wait.\n" +
		"No remote API credentials are used.",
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.LoadForMaintenance(serverConfigPath())
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		result, err := upgrade.RequestReload(ctx, cfg.Server.PIDFile, reloadWait, cfg.Server.UpgradeTimeout)
		if err != nil {
			return err
		}
		return cli.RenderAction(cmd, "reloaded", map[string]any{"previous_pid": result.PreviousPID, "pid": result.PID},
			fmt.Sprintf("reload succeeded; successor PID %d is ready", result.PID))
	},
}
