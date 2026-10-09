// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/podomy/concord/internal/transport"
)

// newNodeCommand creates the 'concord node' command group.
func newNodeCommand() *cobra.Command {
	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Inspect cluster membership and cluster mesh",
	}

	nodeCmd.AddCommand(newNodeListCommand())
	nodeCmd.AddCommand(newNodeTrailCommand())
	nodeCmd.AddCommand(newNodeRotateKeyCommand())

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
	_, _ = fmt.Fprintln(tw, "NODE ID\tADDRESS\tSTATE\tWIREGUARD PUBLIC KEY\tPRESSURE\tLAT\tLON\tANCHOR") //nolint:errcheck // CLI output
	for _, n := range nodes {
		pressure := max(n.CPUPercent, n.MemPercent, n.DiskPercent)
		anchor := "-"
		if n.Anchor {
			anchor = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d%%\t%.4f\t%.4f\t%s\n", n.ID, n.Address, n.State, dashIfEmpty(n.WireGuardPublicKey), pressure, n.Lat, n.Lon, anchor) //nolint:errcheck // CLI output
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush node table: %w", err)
	}

	return nil
}

// newNodeTrailCommand creates the 'concord node trail' subcommand.
func newNodeTrailCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "trail",
		Short: "Show this node's recorded positions oldest-first",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return handleNodeTrail(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

// handleNodeTrail prints timestamped coordinates, one per line. Empty
// trail prints nothing and succeeds: a stationary node has nowhere to go.
func handleNodeTrail(ctx context.Context, stdout io.Writer) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	trail, err := client.Trail(ctx)
	if err != nil {
		return fmt.Errorf("get node trail: %w", err)
	}

	for _, p := range trail {
		_, _ = fmt.Fprintf(stdout, "%s %.6f %.6f\n", p.At.Format(time.RFC3339), p.Lat, p.Lon) //nolint:errcheck // CLI output
	}

	return nil
}

// newNodeRotateKeyCommand creates the 'concord node rotate-key' subcommand.
func newNodeRotateKeyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "rotate-key",
		Short: "Rotate this node's Noise static key",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return handleNodeRotateKey(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

// handleNodeRotateKey deletes the static key and bumps the generation
// counter. Local file ops on this node only, no daemon involved: it works
// with the daemon stopped, and the new identity takes effect on daemon
// restart.
func handleNodeRotateKey(ctx context.Context, stdout io.Writer) error {
	generation, err := transport.RotateKey(ctx)
	if err != nil {
		return fmt.Errorf("rotate noise key: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "Rotated Noise key to generation %d. Restart the concord daemon to apply.\n", generation) //nolint:errcheck // CLI output
	return nil
}
