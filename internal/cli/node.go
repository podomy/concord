// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// newNodeCommand creates the 'concord node' command group.
func newNodeCommand() *cobra.Command {
	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Inspect cluster membership and cluster mesh",
	}

	nodeCmd.AddCommand(newNodeListCommand())

	return nodeCmd
}

// newNodeListCommand creates the 'concord node list' subcommand.
func newNodeListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List cluster nodes and cluster mesh status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return handleNodeList(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

// handleNodeList queries and displays cluster nodes in a clean table.
func handleNodeList(ctx context.Context, stdout io.Writer) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	nodes, err := client.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("list cluster nodes: %w", err)
	}

	if len(nodes) == 0 {
		_, _ = fmt.Fprintln(stdout, "No cluster nodes found.") //nolint:errcheck // CLI output
		return nil
	}

	tw := newTableWriter(stdout)
	_, _ = fmt.Fprintln(tw, "NODE ID\tADDRESS\tSTATE\tWIREGUARD PUBLIC KEY") //nolint:errcheck // CLI output
	for _, n := range nodes {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.ID, n.Address, n.State, dashIfEmpty(n.WireGuardPublicKey)) //nolint:errcheck // CLI output
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush node table: %w", err)
	}

	return nil
}
