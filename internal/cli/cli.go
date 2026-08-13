// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"io"

	"github.com/spf13/cobra"
)

// NewRootCommand creates the top-level 'concord' Cobra command.
func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "concord",
		Short: "Concord: Autonomous Fleet Orchestrator",
		Long: `Concord is a lightweight, partition-tolerant container orchestrator designed
for autonomous fleets, edge devices, and localized clusters.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.AddCommand(
		newWorkloadCommand(),
		newNodeCommand(),
	)

	return rootCmd
}

// Execute executes the Concord CLI with custom arguments and standard I/O streams.
//
//nolint:contextcheck // Cobra propagates context dynamically via ExecuteContext(ctx)
func Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := NewRootCommand()
	cmd.SetArgs(args)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	return cmd.ExecuteContext(ctx) //nolint:wrapcheck // CLI root entrypoint
}
