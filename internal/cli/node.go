// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/podomy/concord/internal/transport"
)

// dispatchNode routes concord node subcommands.
func dispatchNode(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	if len(args) == 0 {
		printNodeHelp(stderr)
		return fmt.Errorf("requires a subcommand: concord node [list|trail|position|rotate-key]") //nolint:perfsprint // plain error, no wrap target
	}
	name := args[0]
	rest := args[1:]
	switch {
	case isHelpRequest(name) || name == "help":
		return printNodeSubHelp(rest, stdout, stderr)
	case name == "list" || name == "ls":
		return runNodeList(ctx, stdout, stderr, rest)
	case name == "trail":
		return runNodeTrail(ctx, stdout, stderr, rest)
	case name == "position":
		return dispatchNodePosition(ctx, stdout, stderr, rest)
	case name == "rotate-key":
		return runNodeRotateKey(ctx, stdout, stderr, rest)
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord node\"\n", name) //nolint:errcheck // CLI output
		printNodeHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord node\"", name)
	}
}

// printNodeHelp writes node group usage to w.
func printNodeHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Inspect cluster membership and cluster mesh")                               //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                                    //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node [command]")                                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Available Commands:")                                                       //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  list        List cluster nodes and cluster mesh status (alias ls)")       //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  position    Report this node's geographic position")                      //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  rotate-key  Rotate this node's Noise static key")                         //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  trail       Show this node's recorded positions oldest-first")            //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, `Use "concord node [command] --help" for more information about a command.`) //nolint:errcheck // CLI output
}

// printNodeSubHelp prints help for one node subcommand or group.
func printNodeSubHelp(path []string, stdout, stderr io.Writer) error {
	if len(path) == 0 {
		printNodeHelp(stdout)
		return nil
	}
	switch path[0] {
	case "list", "ls":
		printNodeListHelp(stdout)
		return nil
	case "trail":
		printNodeTrailHelp(stdout)
		return nil
	case "position":
		return printNodePositionSubHelp(path[1:], stdout, stderr)
	case "rotate-key":
		printNodeRotateKeyHelp(stdout)
		return nil
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord node\"\n", path[0]) //nolint:errcheck // CLI output
		printNodeHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord node\"", path[0])
	}
}

// printNodeListHelp writes node list usage to w.
func printNodeListHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "List cluster nodes and cluster mesh status") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                           //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node list")                        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                           //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Aliases:")                                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  ls")                                       //nolint:errcheck // CLI output
}

// runNodeList prints cluster nodes after handling list flags.
func runNodeList(ctx context.Context, stdout, _ io.Writer, args []string) error {
	for _, a := range args {
		if isHelpRequest(a) {
			printNodeListHelp(stdout)
			return nil
		}
		if isFlagToken(a) {
			return fmt.Errorf("unknown flag: %s", a)
		}
	}
	return handleNodeList(ctx, stdout)
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

// printNodeTrailHelp writes node trail usage to w.
func printNodeTrailHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Show this node's recorded positions oldest-first") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                 //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                           //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node trail")                             //nolint:errcheck // CLI output
}

// runNodeTrail prints recorded positions after handling trail flags.
func runNodeTrail(ctx context.Context, stdout, _ io.Writer, args []string) error {
	for _, a := range args {
		if isHelpRequest(a) {
			printNodeTrailHelp(stdout)
			return nil
		}
		if isFlagToken(a) {
			return fmt.Errorf("unknown flag: %s", a)
		}
	}
	return handleNodeTrail(ctx, stdout)
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

// dispatchNodePosition routes concord node position subcommands.
func dispatchNodePosition(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	if len(args) == 0 {
		printNodePositionHelp(stderr)
		return fmt.Errorf("requires a subcommand: concord node position [set]") //nolint:perfsprint // plain error, no wrap target
	}
	name := args[0]
	rest := args[1:]
	switch {
	case isHelpRequest(name) || name == "help":
		return printNodePositionSubHelp(rest, stdout, stderr)
	case name == "set":
		return runNodePositionSet(ctx, stdout, stderr, rest)
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord node position\"\n", name) //nolint:errcheck // CLI output
		printNodePositionHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord node position\"", name)
	}
}

// printNodePositionHelp writes node position group usage to w.
func printNodePositionHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Report this node's geographic position")                                             //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                                             //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node position [command]")                                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Available Commands:")                                                                //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  set         Apply one live position fix")                                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, `Use "concord node position [command] --help" for more information about a command.`) //nolint:errcheck // CLI output
}

// printNodePositionSubHelp prints help for the position set command.
func printNodePositionSubHelp(path []string, stdout, stderr io.Writer) error {
	if len(path) == 0 {
		printNodePositionHelp(stdout)
		return nil
	}
	if path[0] == "set" {
		printNodePositionSetHelp(stdout)
		return nil
	}
	_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord node position\"\n", path[0]) //nolint:errcheck // CLI output
	printNodePositionHelp(stderr)
	return fmt.Errorf("unknown command %q for \"concord node position\"", path[0])
}

// nodePositionSetTakesValue marks position set flags that consume a value.
var nodePositionSetTakesValue = map[string]bool{"lat": true, "lon": true}

// newNodePositionSetFlagSet binds position flags and documents them.
func newNodePositionSetFlagSet(lat, lon *float64) (*flag.FlagSet, []flagDoc) {
	fs := flag.NewFlagSet("concord node position set", flag.ContinueOnError)
	fs.Float64Var(lat, "lat", 0, "latitude in decimal degrees")
	fs.Float64Var(lon, "lon", 0, "longitude in decimal degrees")
	docs := []flagDoc{
		{long: "lat", kind: "float", def: "0", usage: "latitude in decimal degrees"},
		{long: "lon", kind: "float", def: "0", usage: "longitude in decimal degrees"},
	}
	return fs, docs
}

// printNodePositionSetHelp writes position set usage and flags to w.
func printNodePositionSetHelp(w io.Writer) {
	var lat, lon float64
	_, docs := newNodePositionSetFlagSet(&lat, &lon)
	_, _ = fmt.Fprintln(w, "Apply one live position fix") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                            //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                      //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node position set") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                            //nolint:errcheck // CLI output
	printFlagDocs(w, docs)
}

// runNodePositionSet parses coordinates and applies one live position fix.
func runNodePositionSet(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	var lat, lon float64
	fs, _ := newNodePositionSetFlagSet(&lat, &lon)
	flagArgs, _, wantHelp := splitArgs(args, nodePositionSetTakesValue)
	if wantHelp {
		printNodePositionSetHelp(stdout)
		return nil
	}
	fs.SetOutput(stderr)
	err := fs.Parse(flagArgs)
	if err != nil {
		return fmt.Errorf("parse node position set flags: %w", err)
	}
	visited := visitedFlags(fs)
	if !visited["lat"] {
		return fmt.Errorf("required flag(s) \"lat\" not set") //nolint:perfsprint // plain error, no wrap target
	}
	if !visited["lon"] {
		return fmt.Errorf("required flag(s) \"lon\" not set") //nolint:perfsprint // plain error, no wrap target
	}
	return handleNodePositionSet(ctx, stdout, lat, lon)
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

// printNodeRotateKeyHelp writes rotate-key usage to w.
func printNodeRotateKeyHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Rotate this node's Noise static key") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                    //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord node rotate-key")           //nolint:errcheck // CLI output
}

// runNodeRotateKey rotates the Noise key after handling rotate-key flags.
func runNodeRotateKey(ctx context.Context, stdout, _ io.Writer, args []string) error {
	for _, a := range args {
		if isHelpRequest(a) {
			printNodeRotateKeyHelp(stdout)
			return nil
		}
		if isFlagToken(a) {
			return fmt.Errorf("unknown flag: %s", a)
		}
	}
	return handleNodeRotateKey(ctx, stdout)
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
