// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/sdk"
	"github.com/podomy/concord/sdk/fileart"
)

// dispatchWorkload routes concord workload subcommands.
func dispatchWorkload(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	if len(args) == 0 {
		printWorkloadHelp(stderr)
		return fmt.Errorf("requires a subcommand: concord workload [run|list|inspect|stats|logs|update|stop]") //nolint:perfsprint // plain error, no wrap target
	}
	name := args[0]
	rest := args[1:]
	if isHelpRequest(name) || name == "help" {
		return printWorkloadSubHelp(rest, stdout, stderr)
	}
	return runWorkloadLeaf(ctx, stdout, stderr, name, rest)
}

// runWorkloadLeaf dispatches one workload leaf after help handling.
func runWorkloadLeaf(ctx context.Context, stdout, stderr io.Writer, name string, rest []string) error {
	switch name {
	case "run":
		return runWorkloadRun(ctx, stdout, stderr, rest)
	case "list", "ls":
		return runWorkloadList(ctx, stdout, stderr, rest)
	case "inspect":
		return runWorkloadInspect(ctx, stdout, stderr, rest)
	case "stats":
		return runWorkloadStats(ctx, stdout, stderr, rest)
	case "logs":
		return runWorkloadLogs(ctx, stdout, stderr, rest)
	case "update":
		return runWorkloadUpdate(ctx, stdout, stderr, rest)
	case "stop":
		return runWorkloadStop(ctx, stdout, stderr, rest)
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord workload\"\n", name) //nolint:errcheck // CLI output
		printWorkloadHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord workload\"", name)
	}
}

// printWorkloadHelp writes workload group usage to w.
func printWorkloadHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Manage container workloads (run, list, inspect, stop)")                         //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                                        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload [command]")                                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Available Commands:")                                                           //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  inspect     Display detailed specification for a workload")                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  list        List all active workloads (alias ls)")                            //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  logs        Show captured container output for a workload")                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  run         Deploy and run a new container workload")                         //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  stats       Display live utilization for a workload")                         //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  stop        Stop and remove a workload from the cluster")                     //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  update      Move a live workload to a new image keeping its identity")        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, `Use "concord workload [command] --help" for more information about a command.`) //nolint:errcheck // CLI output
}

// printWorkloadSubHelp prints help for one workload leaf command.
func printWorkloadSubHelp(path []string, stdout, stderr io.Writer) error {
	if len(path) == 0 {
		printWorkloadHelp(stdout)
		return nil
	}
	switch path[0] {
	case "run":
		printWorkloadRunHelp(stdout)
		return nil
	case "list", "ls":
		printWorkloadListHelp(stdout)
		return nil
	case "inspect":
		printWorkloadInspectHelp(stdout)
		return nil
	case "stats":
		printWorkloadStatsHelp(stdout)
		return nil
	case "logs":
		printWorkloadLogsHelp(stdout)
		return nil
	case "update":
		printWorkloadUpdateHelp(stdout)
		return nil
	case "stop":
		printWorkloadStopHelp(stdout)
		return nil
	default:
		_, _ = fmt.Fprintf(stderr, "Error: unknown command %q for \"concord workload\"\n", path[0]) //nolint:errcheck // CLI output
		printWorkloadHelp(stderr)
		return fmt.Errorf("unknown command %q for \"concord workload\"", path[0])
	}
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

// workloadRunTakesValue marks workload run flags that consume a value.
var workloadRunTakesValue = map[string]bool{
	"port": true, "p": true,
	"env": true, "e": true,
	"restart": true, "cpu": true, "memory": true,
	"health-path": true, "health-action": true, "health-probe": true, "health-cmd": true,
	"depends-on": true, "qos": true, "file": true,
}

// newWorkloadRunFlagSet binds run flags to rf and documents them for help.
func newWorkloadRunFlagSet(rf *runFlags, env, depends, files *stringArray) (*flag.FlagSet, []flagDoc) {
	fs := flag.NewFlagSet("concord workload run", flag.ContinueOnError)
	fs.StringVar(&rf.port, "port", "", "Port mapping (host:container, e.g. 8080:80)")
	fs.StringVar(&rf.port, "p", "", "Port mapping (host:container, e.g. 8080:80)")
	fs.Var(env, "env", "Environment variable in KEY=VALUE format (repeatable)")
	fs.Var(env, "e", "Environment variable in KEY=VALUE format (repeatable)")
	fs.StringVar(&rf.restart, "restart", "always", "Restart policy: 'always', 'never', or 'on_failure'")
	fs.Uint64Var(&rf.cpu, "cpu", 1024, "CFS CPU shares (1024 = 1 core)")
	fs.Int64Var(&rf.memory, "memory", 0, "Memory limit in MB (0 = unlimited)")
	fs.StringVar(&rf.healthPath, "health-path", "/health", "HTTP health check endpoint")
	fs.StringVar(&rf.healthAction, "health-action", "restart", "Action on health check failure: 'restart' or 'signal'")
	fs.StringVar(&rf.healthProbe, "health-probe", "http", "Health check type: 'http', 'tcp', or 'exec'")
	fs.StringVar(&rf.healthCmd, "health-cmd", "", "Exec probe command, split on whitespace")
	fs.Var(depends, "depends-on", "Wait for this workload ID or prefix on the same node (repeatable)")
	fs.StringVar(&rf.qos, "qos", "besteffort", "Compute class: 'besteffort', 'burstable', or 'guaranteed'")
	fs.Var(files, "file", "Stage a file: --file /container/path=/local/path[:mode] (repeatable)")
	docs := []flagDoc{
		{long: "port", short: "p", kind: "string", def: `""`, usage: "Port mapping (host:container, e.g. 8080:80)"},
		{long: "env", short: "e", kind: "stringArray", def: "[]", usage: "Environment variable in KEY=VALUE format (repeatable)"},
		{long: "restart", kind: "string", def: `"always"`, usage: "Restart policy: 'always', 'never', or 'on_failure'"},
		{long: "cpu", kind: "uint", def: "1024", usage: "CFS CPU shares (1024 = 1 core)"},
		{long: "memory", kind: "int", def: "0", usage: "Memory limit in MB (0 = unlimited)"},
		{long: "health-path", kind: "string", def: `"/health"`, usage: "HTTP health check endpoint"},
		{long: "health-action", kind: "string", def: `"restart"`, usage: "Action on health check failure: 'restart' or 'signal'"},
		{long: "health-probe", kind: "string", def: `"http"`, usage: "Health check type: 'http', 'tcp', or 'exec'"},
		{long: "health-cmd", kind: "string", def: `""`, usage: "Exec probe command, split on whitespace"},
		{long: "depends-on", kind: "stringArray", def: "[]", usage: "Wait for this workload ID or prefix on the same node (repeatable)"},
		{long: "qos", kind: "string", def: `"besteffort"`, usage: "Compute class: 'besteffort', 'burstable', or 'guaranteed'"},
		{long: "file", kind: "stringArray", def: "[]", usage: "Stage a file: --file /container/path=/local/path[:mode] (repeatable)"},
	}
	return fs, docs
}

// printWorkloadRunHelp writes run usage and flags to w.
// Command after -- is passed verbatim and not parsed as flags.
func printWorkloadRunHelp(w io.Writer) {
	var rf runFlags
	var env, depends, files stringArray
	_, docs := newWorkloadRunFlagSet(&rf, &env, &depends, &files)
	_, _ = fmt.Fprintln(w, "Deploy and run a new container workload")                      //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                             //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                       //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload run [flags] <image> [command...]")          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                             //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Command after -- is passed verbatim and not parsed as flags.") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                             //nolint:errcheck // CLI output
	printFlagDocs(w, docs)
}

// runWorkloadRun parses run flags and submits a new workload.
func runWorkloadRun(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	var rf runFlags
	var env, depends, files stringArray
	fs, _ := newWorkloadRunFlagSet(&rf, &env, &depends, &files)
	flagArgs, positional, wantHelp := splitArgs(args, workloadRunTakesValue)
	if wantHelp {
		printWorkloadRunHelp(stdout)
		return nil
	}
	fs.SetOutput(stderr)
	err := fs.Parse(flagArgs)
	if err != nil {
		return fmt.Errorf("parse workload run flags: %w", err)
	}
	rf.envFlags = []string(env)
	rf.dependsOn = []string(depends)
	rf.fileFlags = []string(files)
	if len(positional) < 1 {
		return fmt.Errorf("requires at least 1 arg(s), only %d received", len(positional))
	}
	return handleWorkloadRun(ctx, stdout, &rf, positional[0], positional[1:])
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

// printWorkloadListHelp writes list usage to w.
func printWorkloadListHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "List all active workloads") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                    //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload list")   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                          //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Aliases:")                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  ls")                      //nolint:errcheck // CLI output
}

// runWorkloadList prints active workloads after handling list flags.
func runWorkloadList(ctx context.Context, stdout, _ io.Writer, args []string) error {
	for _, a := range args {
		if isHelpRequest(a) {
			printWorkloadListHelp(stdout)
			return nil
		}
		if isFlagToken(a) {
			return fmt.Errorf("unknown flag: %s", a)
		}
	}
	return handleWorkloadList(ctx, stdout)
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

// printWorkloadInspectHelp writes inspect usage to w.
func printWorkloadInspectHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Display detailed specification for a workload") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload inspect <id>")               //nolint:errcheck // CLI output
}

// runWorkloadInspect validates the id argument and prints the workload spec.
func runWorkloadInspect(ctx context.Context, stdout, _ io.Writer, args []string) error {
	flagArgs, positional, wantHelp := splitArgs(args, nil)
	if wantHelp {
		printWorkloadInspectHelp(stdout)
		return nil
	}
	if len(flagArgs) > 0 {
		return fmt.Errorf("unknown flag: %s", flagArgs[0])
	}
	if len(positional) != 1 {
		return fmt.Errorf("accepts 1 arg(s), received %d", len(positional))
	}
	return handleWorkloadInspect(ctx, stdout, positional[0])
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

// printWorkloadStatsHelp writes stats usage to w.
func printWorkloadStatsHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Display live utilization for a workload") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload stats <id>")           //nolint:errcheck // CLI output
}

// runWorkloadStats validates the id argument and prints live utilization.
func runWorkloadStats(ctx context.Context, stdout, _ io.Writer, args []string) error {
	flagArgs, positional, wantHelp := splitArgs(args, nil)
	if wantHelp {
		printWorkloadStatsHelp(stdout)
		return nil
	}
	if len(flagArgs) > 0 {
		return fmt.Errorf("unknown flag: %s", flagArgs[0])
	}
	if len(positional) != 1 {
		return fmt.Errorf("accepts 1 arg(s), received %d", len(positional))
	}
	return handleWorkloadStats(ctx, stdout, positional[0])
}

// workloadLogsTakesValue marks workload logs flags that consume a value.
var workloadLogsTakesValue = map[string]bool{"tail": true}

// newWorkloadLogsFlagSet binds logs flags and documents them for help.
func newWorkloadLogsFlagSet(tail *int, follow *bool) (*flag.FlagSet, []flagDoc) {
	fs := flag.NewFlagSet("concord workload logs", flag.ContinueOnError)
	fs.IntVar(tail, "tail", 0, "show only the last N lines (0 means all)")
	fs.BoolVar(follow, "follow", false, "keep printing new lines until interrupted")
	fs.BoolVar(follow, "f", false, "keep printing new lines until interrupted")
	docs := []flagDoc{
		{long: "tail", kind: "int", def: "0", usage: "show only the last N lines (0 means all)"},
		{long: "follow", short: "f", kind: "bool", def: "false", usage: "keep printing new lines until interrupted"},
	}
	return fs, docs
}

// printWorkloadLogsHelp writes logs usage and flags to w.
func printWorkloadLogsHelp(w io.Writer) {
	var tail int
	var follow bool
	_, docs := newWorkloadLogsFlagSet(&tail, &follow)
	_, _ = fmt.Fprintln(w, "Show captured container output for a workload") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                        //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload logs <id>")                  //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                              //nolint:errcheck // CLI output
	printFlagDocs(w, docs)
}

// runWorkloadLogs parses logs flags and prints captured container output.
func runWorkloadLogs(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	var tail int
	var follow bool
	fs, _ := newWorkloadLogsFlagSet(&tail, &follow)
	flagArgs, positional, wantHelp := splitArgs(args, workloadLogsTakesValue)
	if wantHelp {
		printWorkloadLogsHelp(stdout)
		return nil
	}
	fs.SetOutput(stderr)
	err := fs.Parse(flagArgs)
	if err != nil {
		return fmt.Errorf("parse workload logs flags: %w", err)
	}
	if len(positional) != 1 {
		return fmt.Errorf("accepts 1 arg(s), received %d", len(positional))
	}
	return handleWorkloadLogs(ctx, stdout, positional[0], tail, follow)
}

// workloadUpdateTakesValue marks workload update flags that consume a value.
var workloadUpdateTakesValue = map[string]bool{"image": true}

// newWorkloadUpdateFlagSet binds the update image flag for parsing and help.
func newWorkloadUpdateFlagSet(image *string) (*flag.FlagSet, []flagDoc) {
	fs := flag.NewFlagSet("concord workload update", flag.ContinueOnError)
	fs.StringVar(image, "image", "", "new image reference (required)")
	docs := []flagDoc{
		{long: "image", kind: "string", def: `""`, usage: "new image reference (required)"},
	}
	return fs, docs
}

// printWorkloadUpdateHelp writes update usage and flags to w.
func printWorkloadUpdateHelp(w io.Writer) {
	var image string
	_, docs := newWorkloadUpdateFlagSet(&image)
	_, _ = fmt.Fprintln(w, "Move a live workload to a new image keeping its identity") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                         //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                                   //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload update <id>")                           //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                         //nolint:errcheck // CLI output
	printFlagDocs(w, docs)
}

// runWorkloadUpdate parses the image flag and moves a live workload.
func runWorkloadUpdate(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	var image string
	fs, _ := newWorkloadUpdateFlagSet(&image)
	flagArgs, positional, wantHelp := splitArgs(args, workloadUpdateTakesValue)
	if wantHelp {
		printWorkloadUpdateHelp(stdout)
		return nil
	}
	fs.SetOutput(stderr)
	err := fs.Parse(flagArgs)
	if err != nil {
		return fmt.Errorf("parse workload update flags: %w", err)
	}
	visited := visitedFlags(fs)
	if !visited["image"] {
		return fmt.Errorf("required flag(s) \"image\" not set") //nolint:perfsprint // plain error, no wrap target
	}
	if len(positional) != 1 {
		return fmt.Errorf("accepts 1 arg(s), received %d", len(positional))
	}
	return handleWorkloadUpdate(ctx, stdout, positional[0], image)
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

// printWorkloadStopHelp writes stop usage to w.
func printWorkloadStopHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Stop and remove a workload from the cluster") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                            //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                      //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord workload stop <id>")                //nolint:errcheck // CLI output
}

// runWorkloadStop validates the id argument and stops the workload.
func runWorkloadStop(ctx context.Context, stdout, _ io.Writer, args []string) error {
	flagArgs, positional, wantHelp := splitArgs(args, nil)
	if wantHelp {
		printWorkloadStopHelp(stdout)
		return nil
	}
	if len(flagArgs) > 0 {
		return fmt.Errorf("unknown flag: %s", flagArgs[0])
	}
	if len(positional) != 1 {
		return fmt.Errorf("accepts 1 arg(s), received %d", len(positional))
	}
	return handleWorkloadStop(ctx, stdout, positional[0])
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
