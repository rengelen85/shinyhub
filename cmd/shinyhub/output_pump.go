package main

import (
	"fmt"
	"github.com/rvben/shinyhub/internal/process"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

func newOutputPumpCmd() *cobra.Command {
	return &cobra.Command{Use: "native-output-pump <path> <max-size>", Hidden: true, Args: cobra.ExactArgs(2), RunE: func(_ *cobra.Command, args []string) error {
		size, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || size <= 0 {
			return fmt.Errorf("invalid log size")
		}
		ready := os.NewFile(3, "output-pump-ready")
		defer ready.Close()
		return process.PumpOutput(args[0], size, os.Stdin, ready)
	}}
}
