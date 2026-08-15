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
		Short: "Concord: Coordination layer for partition-tolerant fleets",
		Long: `Concord is a decentralized coordination layer for distributed systems operating
under unreliable, intermittent, and partitioned networks.`,
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
