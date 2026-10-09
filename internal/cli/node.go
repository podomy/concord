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
	nodeCmd.AddCommand(newNodePositionCommand())
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

// newNodePositionCommand creates the 'concord node position' command group.
func newNodePositionCommand() *cobra.Command {
	posCmd := &cobra.Command{
		Use:   "position",
		Short: "Report this node's geographic position",
	}

	setCmd := &cobra.Command{
		Use:   "set",
		Short: "Apply one live position fix",
		RunE: func(cmd *cobra.Command, _ []string) error {
			lat, err := cmd.Flags().GetFloat64("lat")
			if err != nil {
				return fmt.Errorf("read --lat flag: %w", err)
			}
			lon, err := cmd.Flags().GetFloat64("lon")
			if err != nil {
				return fmt.Errorf("read --lon flag: %w", err)
			}
			return handleNodePositionSet(cmd.Context(), cmd.OutOrStdout(), lat, lon)
		},
	}
	setCmd.Flags().Float64("lat", 0, "latitude in decimal degrees")
	setCmd.Flags().Float64("lon", 0, "longitude in decimal degrees")
	_ = setCmd.MarkFlagRequired("lat") //nolint:errcheck // required-flag setup cannot fail usefully here
	_ = setCmd.MarkFlagRequired("lon") //nolint:errcheck // required-flag setup cannot fail usefully here
	posCmd.AddCommand(setCmd)

	return posCmd
}

// handleNodePositionSet applies one live fix through the daemon: persisted
// for reboot, gossiped to the fleet, recorded to trail and track log.
// This is the only position writer; the config file is never polled.
func handleNodePositionSet(ctx context.Context, stdout io.Writer, lat, lon float64) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	if err := client.SetPosition(ctx, lat, lon); err != nil {
		return fmt.Errorf("set node position: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "Position set to %.6f %.6f.\n", lat, lon) //nolint:errcheck // CLI output
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
