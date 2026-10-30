// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/sdk"
	"github.com/podomy/concord/sdk/fileart"
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
		newWorkloadStatsCommand(),
		newWorkloadLogsCommand(),
		newWorkloadUpdateCommand(),
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
	healthProbe  string
	healthCmd    string
	dependsOn    []string
	qos          string
	fileFlags    []string
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
	flags.StringVar(&rf.healthProbe, "health-probe", "http", "Health check type: 'http', 'tcp', or 'exec'")
	flags.StringVar(&rf.healthCmd, "health-cmd", "", `Exec probe command, split on whitespace: --health-cmd "/bin/check --ready" runs /bin/check --ready`)
	flags.StringArrayVar(&rf.dependsOn, "depends-on", nil, "Wait for this workload ID or prefix on the same node (repeatable)")
	flags.StringVar(&rf.qos, "qos", "besteffort", "Compute class: 'besteffort', 'burstable', or 'guaranteed'")
	flags.StringArrayVar(&rf.fileFlags, "file", nil, "Stage a file: --file /container/path=/local/path[:mode] (repeatable)")

	return cmd
}

// parseHealthFlags validates the health flag trio into a probe, an action,
// and the exec argv. Exec without a command fails here, at the edge,
// instead of restart-looping a misconfigured workload later.
func parseHealthFlags(rf *runFlags) (sdk.HealthProbe, sdk.HealthAction, []string, error) {
	var action sdk.HealthAction
	switch strings.ToLower(strings.TrimSpace(rf.healthAction)) {
	case "restart", "":
		action = sdk.HealthActionRestart
	case "signal":
		action = sdk.HealthActionSignal
	default:
		return "", 0, nil, fmt.Errorf("invalid health action %q; must be 'restart' or 'signal'", rf.healthAction)
	}

	var probe sdk.HealthProbe
	switch strings.ToLower(strings.TrimSpace(rf.healthProbe)) {
	case "http", "":
		probe = sdk.HealthProbeHTTP
	case "tcp":
		probe = sdk.HealthProbeTCP
	case "exec":
		probe = sdk.HealthProbeExec
	default:
		return "", 0, nil, fmt.Errorf("invalid health probe %q; must be 'http', 'tcp', or 'exec'", rf.healthProbe)
	}
	if probe == sdk.HealthProbeExec && len(strings.Fields(rf.healthCmd)) == 0 {
		return "", 0, nil, ErrHealthCommandRequired
	}

	return probe, action, strings.Fields(rf.healthCmd), nil
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

	probe, action, healthCmd, err := parseHealthFlags(rf)
	if err != nil {
		return sdk.Workload{}, err
	}

	var qos sdk.QoS
	switch strings.ToLower(strings.TrimSpace(rf.qos)) {
	case "besteffort", "":
		qos = sdk.QoSBestEffort
	case "burstable":
		qos = sdk.QoSBurstable
	case "guaranteed":
		qos = sdk.QoSGuaranteed
	default:
		return sdk.Workload{}, fmt.Errorf("invalid QoS class %q; must be 'besteffort', 'burstable', or 'guaranteed'", rf.qos)
	}

	builder := sdk.NewWorkload().
		Image(image).
		Command(command...).
		Restart(sdk.RestartPolicy(rf.restart)).
		CPUShares(rf.cpu).
		MemoryMB(rf.memory).
		HealthCheck(rf.healthPath, action).
		HealthProbe(probe, healthCmd...).
		QoS(qos)

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

// parseFileFlag splits one --file flag into container path, local path,
// and mode without touching the network. Mode defaults to 0 (0600 at
// materialize); anything above 0777 fails here.
func parseFileFlag(flag string) (container, local string, mode uint32, err error) {
	container, local, ok := strings.Cut(flag, "=")
	if !ok || container == "" || local == "" {
		return "", "", 0, fmt.Errorf("invalid --file %q; want /container/path=/local/path[:mode]", flag)
	}
	if base, modeStr, ok := strings.Cut(local, ":"); ok {
		parsed, perr := strconv.ParseUint(modeStr, 8, 32)
		if perr != nil || parsed > 0o777 {
			return "", "", 0, fmt.Errorf("invalid --file mode %q; want octal up to 0777", modeStr)
		}
		local = base
		mode = uint32(parsed)
	}
	return container, local, mode, nil
}

// pushFileFlag parses one --file flag, pushes the local bytes as an
// artifact, and returns the spec pointer.
func pushFileFlag(ctx context.Context, artifacts *fileart.Client, flag string) (sdk.FileRef, error) {
	container, local, mode, err := parseFileFlag(flag)
	if err != nil {
		return sdk.FileRef{}, err
	}
	content, err := os.ReadFile(filepath.Clean(local))
	if err != nil {
		return sdk.FileRef{}, fmt.Errorf("read file %q: %w", local, err)
	}
	digest, err := artifacts.Push(ctx, content)
	if err != nil {
		return sdk.FileRef{}, fmt.Errorf("push artifact: %w", err)
	}
	return sdk.FileRef{Path: container, Digest: digest, Mode: mode}, nil
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

	if len(rf.dependsOn) > 0 {
		for _, dep := range rf.dependsOn {
			depID, err := resolveWorkloadID(ctx, client, dep)
			if err != nil {
				return fmt.Errorf("resolve dependency %q: %w", dep, err)
			}
			workload.DependsOn = append(workload.DependsOn, depID)
		}
	}

	// Files push to the local registry before submit: the spec carries
	// digests only, and a failed push fails the run instead of
	// submitting a workload that can never start.
	if len(rf.fileFlags) > 0 {
		artifacts := fileart.NewClient("127.0.0.1:" + strconv.Itoa(registry.Port))
		for _, flag := range rf.fileFlags {
			ref, err := pushFileFlag(ctx, artifacts, flag)
			if err != nil {
				return err
			}
			workload.Files = append(workload.Files, ref)
		}
	}

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

func newWorkloadStatsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stats <id>",
		Short: "Display live utilization for a workload",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleWorkloadStats(cmd.Context(), cmd.OutOrStdout(), args[0])
		},
	}
}

// newWorkloadLogsCommand creates the 'concord workload logs' subcommand.
func newWorkloadLogsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <id>",
		Short: "Show captured container output for a workload",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tail, err := cmd.Flags().GetInt("tail")
			if err != nil {
				return fmt.Errorf("read --tail flag: %w", err)
			}
			follow, err := cmd.Flags().GetBool("follow")
			if err != nil {
				return fmt.Errorf("read --follow flag: %w", err)
			}
			return handleWorkloadLogs(cmd.Context(), cmd.OutOrStdout(), args[0], tail, follow)
		},
	}
	cmd.Flags().Int("tail", 0, "show only the last N lines (0 means all)")
	cmd.Flags().BoolP("follow", "f", false, "keep printing new lines until interrupted")
	return cmd
}

// newWorkloadUpdateCommand creates the 'concord workload update' subcommand.
func newWorkloadUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <id>",
		Short: "Move a live workload to a new image keeping its identity",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			image, err := cmd.Flags().GetString("image")
			if err != nil {
				return fmt.Errorf("read --image flag: %w", err)
			}
			return handleWorkloadUpdate(cmd.Context(), cmd.OutOrStdout(), args[0], image)
		},
	}
	cmd.Flags().String("image", "", "new image reference (required)")
	_ = cmd.MarkFlagRequired("image") //nolint:errcheck // required-flag setup cannot fail usefully here
	return cmd
}

// handleWorkloadUpdate bumps a live workload to a new image. Identity,
// assignment, and history continue; rollback is another bump.
func handleWorkloadUpdate(ctx context.Context, stdout io.Writer, idArg, image string) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	targetID, err := resolveWorkloadID(ctx, client, idArg)
	if err != nil {
		return err
	}

	if err := client.UpdateImage(ctx, targetID, image); err != nil {
		return fmt.Errorf("update workload image %s: %w", targetID, err)
	}

	_, _ = fmt.Fprintf(stdout, "Workload %s updating to %s.\n", truncateID(targetID), image) //nolint:errcheck // CLI output
	return nil
}

// handleWorkloadLogs prints captured output lines oldest-first. Follow
// polls the snapshot every second and prints lines newer than the last
// printed timestamp; ring overwrites can only drop older lines, never
// reorder or duplicate, so timestamp comparison is exact.
func handleWorkloadLogs(ctx context.Context, stdout io.Writer, idArg string, tail int, follow bool) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	targetID, err := resolveWorkloadID(ctx, client, idArg)
	if err != nil {
		return err
	}

	printed, err := printLogLines(ctx, client, stdout, targetID, tail, time.Time{})
	if err != nil {
		return err
	}
	if !follow {
		return nil
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			printed, err = printLogLines(ctx, client, stdout, targetID, 0, printed)
			if err != nil {
				return err
			}
		}
	}
}

// printLogLines prints lines newer than since and returns the newest
// printed timestamp. Tail applies only to the first (snapshot) call:
// follow-up polls fetch everything and filter by timestamp.
func printLogLines(ctx context.Context, client sdk.Client, stdout io.Writer, targetID uuid.UUID, tail int, since time.Time) (time.Time, error) {
	lines, err := client.Logs(ctx, targetID, tail)
	if err != nil {
		return since, fmt.Errorf("get workload logs %s: %w", targetID, err)
	}

	for _, line := range lines {
		if !line.At.After(since) {
			continue
		}
		_, _ = fmt.Fprintln(stdout, line.Line) //nolint:errcheck // CLI output
		since = line.At
	}
	return since, nil
}

func handleWorkloadStats(ctx context.Context, stdout io.Writer, idArg string) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	targetID, err := resolveWorkloadID(ctx, client, idArg)
	if err != nil {
		return err
	}

	stats, err := client.Stats(ctx, targetID)
	if err != nil {
		return fmt.Errorf("get workload stats %s: %w", targetID, err)
	}

	_, _ = fmt.Fprintf(stdout, "Workload %s\n", truncateID(targetID))                                //nolint:errcheck // CLI output
	_, _ = fmt.Fprintf(stdout, "  CPU     %d%% (avg %d%%)\n", stats.CPUPercent, stats.AvgCPUPercent) //nolint:errcheck // CLI output
	if stats.MemLimitMB > 0 {
		_, _ = fmt.Fprintf(stdout, "  Memory  %d%% (%d of %d MB, avg %d%%)\n", stats.MemPercent, stats.MemUsageMB, stats.MemLimitMB, stats.AvgMemPercent) //nolint:errcheck // CLI output
	} else {
		_, _ = fmt.Fprintf(stdout, "  Memory  %d%% (%d MB, avg %d%%)\n", stats.MemPercent, stats.MemUsageMB, stats.AvgMemPercent) //nolint:errcheck // CLI output
	}
	_, _ = fmt.Fprintf(stdout, "Node %s\n", truncateID(stats.Node.ID))     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintf(stdout, "  CPU     %d%%\n", stats.Node.CPUPercent)  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintf(stdout, "  Memory  %d%%\n", stats.Node.MemPercent)  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintf(stdout, "  Disk    %d%%\n", stats.Node.DiskPercent) //nolint:errcheck // CLI output
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
