// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/podomy/concord/sdk"
)

// newWorkloadCommand creates the 'concord workload' command group.
func newWorkloadCommand() *cobra.Command {
	workloadCmd := &cobra.Command{
		Use:   "workload",
		Short: "Manage container workloads (run, list, inspect, stop)",
	}

	workloadCmd.AddCommand(
		newWorkloadRunCommand(),
		newWorkloadListCommand(),
		newWorkloadInspectCommand(),
		newWorkloadStopCommand(),
	)

	return workloadCmd
}

type runFlags struct {
	port         string
	envFlags     []string
	restart      string
	cpu          uint64
	memory       int64
	healthPath   string
	healthAction string
}

func newWorkloadRunCommand() *cobra.Command {
	var rf runFlags

	cmd := &cobra.Command{
		Use:   "run [flags] <image> [command...]",
		Short: "Deploy and run a new container workload",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleWorkloadRun(cmd.Context(), cmd.OutOrStdout(), &rf, args[0], args[1:])
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&rf.port, "port", "p", "", "Port mapping (host:container, e.g. 8080:80)")
	flags.StringArrayVarP(&rf.envFlags, "env", "e", nil, "Environment variable in KEY=VALUE format (repeatable)")
	flags.StringVar(&rf.restart, "restart", "always", "Restart policy: 'always', 'never', or 'on_failure'")
	flags.Uint64Var(&rf.cpu, "cpu", 1024, "CFS CPU shares (1024 = 1 core)")
	flags.Int64Var(&rf.memory, "memory", 0, "Memory limit in MB (0 = unlimited)")
	flags.StringVar(&rf.healthPath, "health-path", "/health", "HTTP health check endpoint")
	flags.StringVar(&rf.healthAction, "health-action", "restart", "Action on health check failure: 'restart' or 'signal'")

	return cmd
}

func buildWorkloadSpec(rf *runFlags, image string, command []string) (sdk.Workload, error) {
	hostPort, containerPort, err := parsePortMapping(rf.port)
	if err != nil {
		return sdk.Workload{}, err
	}

	envMap, err := parseEnvVars(rf.envFlags)
	if err != nil {
		return sdk.Workload{}, err
	}

	var action sdk.HealthAction
	switch strings.ToLower(strings.TrimSpace(rf.healthAction)) {
	case "restart", "":
		action = sdk.HealthActionRestart
	case "signal":
		action = sdk.HealthActionSignal
	default:
		return sdk.Workload{}, fmt.Errorf("invalid health action %q; must be 'restart' or 'signal'", rf.healthAction)
	}

	builder := sdk.NewWorkload().
		Image(image).
		Command(command...).
		Restart(sdk.RestartPolicy(rf.restart)).
		CPUShares(rf.cpu).
		MemoryMB(rf.memory).
		HealthCheck(rf.healthPath, action)

	if hostPort > 0 && containerPort > 0 {
		builder = builder.Port(hostPort, containerPort)
	}

	for k, v := range envMap {
		builder = builder.Env(k, v)
	}

	workload, err := builder.Build()
	if err != nil {
		return sdk.Workload{}, fmt.Errorf("build workload spec: %w", err)
	}

	return workload, nil
}

func handleWorkloadRun(ctx context.Context, stdout io.Writer, rf *runFlags, image string, command []string) error {
	workload, err := buildWorkloadSpec(rf, image, command)
	if err != nil {
		return err
	}

	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	id, err := client.Submit(ctx, workload)
	if err != nil {
		return fmt.Errorf("submit workload: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "Submitted workload %s\n", id) //nolint:errcheck // CLI output
	return nil
}

func newWorkloadListCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all active workloads",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return handleWorkloadList(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

func handleWorkloadList(ctx context.Context, stdout io.Writer) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	workloads, err := client.List(ctx)
	if err != nil {
		return fmt.Errorf("list workloads: %w", err)
	}

	if len(workloads) == 0 {
		_, _ = fmt.Fprintln(stdout, "No active workloads found.") //nolint:errcheck // CLI output
		return nil
	}

	tw := newTableWriter(stdout)
	_, _ = fmt.Fprintln(tw, "ID\tIMAGE\tPORTS\tRESTART\tHEALTH") //nolint:errcheck // CLI output

	for _, w := range workloads {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", //nolint:errcheck // CLI output
			truncateID(w.ID),
			w.Image,
			formatPorts(w.HostPort, w.ContainerPort),
			w.Restart,
			dashIfEmpty(w.HealthPath),
		)
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush output table: %w", err)
	}

	return nil
}

func newWorkloadInspectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <id>",
		Short: "Display detailed specification for a workload",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleWorkloadInspect(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
}

func handleWorkloadInspect(ctx context.Context, stdout io.Writer, idArg string) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	targetID, err := resolveWorkloadID(ctx, client, idArg)
	if err != nil {
		return err
	}

	workload, err := client.Get(ctx, targetID)
	if err != nil {
		return fmt.Errorf("get workload %s: %w", targetID, err)
	}

	data, err := json.MarshalIndent(workload, "", "  ")
	if err != nil {
		return fmt.Errorf("format workload json: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "%s\n", data) //nolint:errcheck // CLI output
	return nil
}

func newWorkloadStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop <id>",
		Short: "Stop and remove a workload from the cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleWorkloadStop(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
}

func handleWorkloadStop(ctx context.Context, stdout io.Writer, idArg string) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	targetID, err := resolveWorkloadID(ctx, client, idArg)
	if err != nil {
		return err
	}

	if err := client.Stop(ctx, targetID); err != nil {
		return fmt.Errorf("stop workload %s: %w", targetID, err)
	}

	_, _ = fmt.Fprintf(stdout, "Stopped workload %s\n", targetID) //nolint:errcheck // CLI output
	return nil
}
