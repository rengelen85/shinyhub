package main

import (
	"fmt"
	"os"
	"time"

	"github.com/rvben/shinyhub/internal/config"
	"github.com/rvben/shinyhub/internal/localtoken"
	"github.com/spf13/cobra"
)

func newLocalTokenCmd() *cobra.Command {
	var scope, path string
	var ttl time.Duration
	cmd := &cobra.Command{Use: "local-token", Short: "Mint a short-lived announcement capability as root on the host", Args: cobra.NoArgs,
		Long: "Request a non-renewable announcement-only token over the live server's private Unix socket. Requires root and server.pid_file. Prints only the token; never saves it. Maximum TTL is 15 minutes. Mint a fresh token to withdraw a notice after a long release.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if os.Geteuid() != 0 {
				return fmt.Errorf("local-token requires root on the server host")
			}
			if path == "" {
				path = serverConfigPath()
			}
			cfg, err := config.LoadForMaintenance(path)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			token, err := localtoken.Request(cmd.Context(), cfg.Server.PIDFile, scope, ttl)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), token)
			return err
		},
	}
	cmd.Flags().StringVar(&scope, "scope", "", "Required capability (announcements)")
	_ = cmd.MarkFlagRequired("scope")
	cmd.Flags().DurationVar(&ttl, "ttl", 10*time.Minute, "Credential lifetime (1s..15m)")
	cmd.Flags().StringVar(&path, "config", "", "Path to server config (overrides SHINYHUB_CONFIG)")
	return cmd
}
