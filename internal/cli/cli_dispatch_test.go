// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/podomy/concord/internal/cli"
)

// dxRun executes the CLI with args and returns captured streams.
//
//nolint:unparam // helper keeps stdout and stderr pair for help parity checks
func dxRun(args []string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	err := cli.Execute(context.Background(), args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// dxRunWithEmptyDaemon isolates the daemon socket so RPCs fail fast.
//
//nolint:unparam // helper keeps stdout and stderr pair for dispatch checks
func dxRunWithEmptyDaemon(t *testing.T, args []string) (string, string, error) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var stdout, stderr bytes.Buffer
	err := cli.Execute(context.Background(), args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

// dxMustContain fails when haystack lacks want.
func dxMustContain(t *testing.T, hay, want, label string) {
	t.Helper()
	if !strings.Contains(hay, want) {
		t.Errorf("%s: missing %q in:\n%s", label, want, hay)
	}
}

// dxMustHelpParity checks -h and --help give identical output.
func dxMustHelpParity(t *testing.T, args []string) {
	t.Helper()
	stdoutH, _, err := dxRun(append(args, "-h"))
	if err != nil {
		t.Fatalf("%v -h failed: %v", args, err)
	}
	stdoutLong, _, err := dxRun(append(args, "--help"))
	if err != nil {
		t.Fatalf("%v --help failed: %v", args, err)
	}
	if stdoutH != stdoutLong {
		t.Errorf("%v: -h differs from --help:\n--- -h ---\n%s\n--- --help ---\n%s", args, stdoutH, stdoutLong)
	}
}

// TestDxDispatchLeavesReachable proves every leaf routes without unknown command.
func TestDxDispatchLeavesReachable(t *testing.T) {
	t.Parallel()
	leaves := []struct {
		args []string
		want string
	}{
		{[]string{"workload", "run", "--help"}, "concord workload run"},
		{[]string{"workload", "list", "--help"}, "concord workload list"},
		{[]string{"workload", "ls", "--help"}, "concord workload list"},
		{[]string{"workload", "inspect", "--help"}, "concord workload inspect"},
		{[]string{"workload", "stats", "--help"}, "concord workload stats"},
		{[]string{"workload", "logs", "--help"}, "concord workload logs"},
		{[]string{"workload", "update", "--help"}, "concord workload update"},
		{[]string{"workload", "stop", "--help"}, "concord workload stop"},
		{[]string{"node", "list", "--help"}, "concord node list"},
		{[]string{"node", "ls", "--help"}, "concord node list"},
		{[]string{"node", "trail", "--help"}, "concord node trail"},
		{[]string{"node", "position", "set", "--help"}, "concord node position set"},
		{[]string{"node", "rotate-key", "--help"}, "concord node rotate-key"},
		{[]string{"metrics", "--help"}, "concord metrics"},
	}
	for _, leaf := range leaves {
		t.Run(strings.Join(leaf.args, "_"), func(t *testing.T) {
			t.Parallel()
			stdout, _, err := dxRun(leaf.args)
			if err != nil {
				t.Fatalf("%v failed: %v", leaf.args, err)
			}
			dxMustContain(t, stdout, leaf.want, strings.Join(leaf.args, " "))
		})
	}
}

// TestDxDispatchWithoutHelpDoesNotReportUnknown proves full paths reach leaves.
func TestDxDispatchWithoutHelpDoesNotReportUnknown(t *testing.T) {
	paths := [][]string{
		{"workload", "list"},
		{"workload", "ls"},
		{"workload", "inspect", "deadbeef"},
		{"workload", "stats", "deadbeef"},
		{"workload", "logs", "deadbeef"},
		{"workload", "stop", "deadbeef"},
		{"node", "list"},
		{"node", "ls"},
		{"node", "trail"},
		{"metrics"},
	}
	for _, args := range paths {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			_, _, err := dxRunWithEmptyDaemon(t, args)
			if err == nil {
				return
			}
			if strings.Contains(err.Error(), "unknown command") {
				t.Errorf("%v: got unknown command error, want leaf dispatch: %v", args, err)
			}
		})
	}
}

// TestDxUnknownCommandsMentionName checks unknown errors echo the bad name.
func TestDxUnknownCommandsMentionName(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"frobnicate"},
		{"workload", "frobnicate"},
		{"node", "frobnicate"},
		{"node", "position", "frobnicate"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			t.Parallel()
			_, stderr, err := dxRun(args)
			if err == nil {
				t.Fatalf("%v: expected error, got nil", args)
			}
			bad := args[len(args)-1]
			if !strings.Contains(err.Error(), bad) {
				t.Errorf("%v: error %q does not mention %q", args, err.Error(), bad)
			}
			if !strings.Contains(err.Error(), "unknown command") {
				t.Errorf("%v: error %q lacks unknown command prefix", args, err.Error())
			}
			_ = stderr
		})
	}
}

// TestDxNoArgsHelp checks bare invocations print help.
func TestDxNoArgsHelp(t *testing.T) {
	t.Parallel()
	stdout, _, err := dxRun(nil)
	if err != nil {
		t.Fatalf("root no-args failed: %v", err)
	}
	dxMustContain(t, stdout, "Available Commands", "root no-args")

	for _, args := range [][]string{{"workload"}, {"node"}, {"node", "position"}} {
		_, _, err := dxRun(args)
		if err == nil {
			t.Errorf("%v: expected requires subcommand error, got nil", args)
		} else if !strings.Contains(err.Error(), "requires a subcommand") {
			t.Errorf("%v: error %q lacks requires a subcommand", args, err.Error())
		}
	}
}

// TestDxRunRequiresImage checks exact missing image text.
func TestDxRunRequiresImage(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"workload", "run"})
	if err == nil {
		t.Fatal("workload run without image: expected error, got nil")
	}
	want := `requires at least 1 arg(s), only 0 received`
	if err.Error() != want {
		t.Errorf("workload run no image = %q, want %q", err.Error(), want)
	}
}

// TestDxInspectArgCounts checks exact 0 and 2 arg errors.
func TestDxInspectArgCounts(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"workload", "inspect"})
	if err == nil {
		t.Fatal("inspect with 0 args: expected error, got nil")
	}
	if err.Error() != "accepts 1 arg(s), received 0" {
		t.Errorf("inspect 0 args = %q", err.Error())
	}
	_, _, err = dxRun([]string{"workload", "inspect", "a", "b"})
	if err == nil {
		t.Fatal("inspect with 2 args: expected error, got nil")
	}
	if err.Error() != "accepts 1 arg(s), received 2" {
		t.Errorf("inspect 2 args = %q", err.Error())
	}
}

// TestDxUpdateRequiresImage checks cobra identical required flag text.
func TestDxUpdateRequiresImage(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"workload", "update", "someid"})
	if err == nil {
		t.Fatal("update without image: expected error, got nil")
	}
	want := `required flag(s) "image" not set`
	if err.Error() != want {
		t.Errorf("update missing image = %q, want %q", err.Error(), want)
	}
}

// TestDxPositionSetRequiresCoords checks lat and lon required text.
func TestDxPositionSetRequiresCoords(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"node", "position", "set", "--lat", "1"})
	if err == nil {
		t.Fatal("position set without lon: expected error, got nil")
	}
	if err.Error() != `required flag(s) "lon" not set` {
		t.Errorf("missing lon = %q", err.Error())
	}
	_, _, err = dxRun([]string{"node", "position", "set", "--lon", "1"})
	if err == nil {
		t.Fatal("position set without lat: expected error, got nil")
	}
	if err.Error() != `required flag(s) "lat" not set` {
		t.Errorf("missing lat = %q", err.Error())
	}
}

// TestDxHelpParity checks -h equals --help for each leaf.
func TestDxHelpParity(t *testing.T) {
	t.Parallel()
	groups := [][]string{
		{"workload", "run"},
		{"workload", "list"},
		{"workload", "inspect"},
		{"workload", "stats"},
		{"workload", "logs"},
		{"workload", "update"},
		{"workload", "stop"},
		{"node", "list"},
		{"node", "trail"},
		{"node", "position", "set"},
		{"node", "rotate-key"},
		{"metrics"},
	}
	for _, args := range groups {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			t.Parallel()
			dxMustHelpParity(t, args)
		})
	}
}

// TestDxHelpListsFlagsWithDefaults checks help shows flags and defaults.
func TestDxHelpListsFlagsWithDefaults(t *testing.T) {
	t.Parallel()
	stdout, _, err := dxRun([]string{"workload", "run", "--help"})
	if err != nil {
		t.Fatalf("run help failed: %v", err)
	}
	for _, want := range []string{
		"--port", "--env", "--restart", "--cpu", "--memory",
		"--health-path", "--health-action", "--health-probe", "--health-cmd",
		"--depends-on", "--qos", "--file", "(default",
	} {
		dxMustContain(t, stdout, want, "workload run help")
	}

	stdout, _, err = dxRun([]string{"workload", "logs", "--help"})
	if err != nil {
		t.Fatalf("logs help failed: %v", err)
	}
	for _, want := range []string{"--tail", "--follow", "-f", "(default"} {
		dxMustContain(t, stdout, want, "workload logs help")
	}

	stdout, _, err = dxRun([]string{"workload", "update", "--help"})
	if err != nil {
		t.Fatalf("update help failed: %v", err)
	}
	dxMustContain(t, stdout, "--image", "workload update help")

	stdout, _, err = dxRun([]string{"node", "position", "set", "--help"})
	if err != nil {
		t.Fatalf("position set help failed: %v", err)
	}
	for _, want := range []string{"--lat", "--lon"} {
		dxMustContain(t, stdout, want, "position set help")
	}
}

// TestDxInvalidValues checks bad port, qos, and unknown flags.
func TestDxInvalidValues(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"workload", "run", "-p", "invalid", "nginx"})
	if err == nil {
		t.Fatal("bad port: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid") || !strings.Contains(strings.ToLower(err.Error()), "port") {
		t.Errorf("bad port error = %q", err.Error())
	}

	_, _, err = dxRun([]string{"workload", "run", "--qos", "platinum", "nginx"})
	if err == nil {
		t.Fatal("bad qos: expected error, got nil")
	}
	dxMustContain(t, err.Error(), "invalid QoS class", "bad qos")

	_, _, err = dxRun([]string{"workload", "run", "--bogus-flag", "nginx"})
	if err == nil {
		t.Fatal("unknown run flag: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "bogus-flag") {
		t.Errorf("unknown run flag error = %q", err.Error())
	}

	_, _, err = dxRun([]string{"workload", "inspect", "--bogus", "someid"})
	if err == nil {
		t.Fatal("unknown inspect flag: expected error, got nil")
	}
	if err.Error() != "unknown flag: --bogus" {
		t.Errorf("unknown inspect flag = %q", err.Error())
	}
}

// TestDxFlagValueForms checks --flag=value equals --flag value.
func TestDxFlagValueForms(t *testing.T) {
	_, _, errEq := dxRun([]string{"workload", "run", "--port=invalid", "nginx"})
	if errEq == nil {
		t.Fatal("--port=invalid: expected error, got nil")
	}
	_, _, errSp := dxRun([]string{"workload", "run", "--port", "invalid", "nginx"})
	if errSp == nil {
		t.Fatal("--port invalid: expected error, got nil")
	}
	if errEq.Error() != errSp.Error() {
		t.Errorf("flag forms differ: = %q vs space %q", errEq.Error(), errSp.Error())
	}
}

// TestDxInterspersedFlags checks flags parse after positionals.
func TestDxInterspersedFlags(t *testing.T) {
	t.Parallel()
	_, _, err := dxRun([]string{"workload", "run", "nginx", "--health-probe", "bogus"})
	if err == nil {
		t.Fatal("interspersed invalid probe: expected error, got nil")
	}
	dxMustContain(t, err.Error(), "invalid health probe", "interspersed")

	_, _, errBefore := dxRun([]string{"workload", "run", "--health-probe", "bogus", "nginx"})
	if errBefore == nil {
		t.Fatal("prefixed invalid probe: expected error, got nil")
	}
	if err.Error() != errBefore.Error() {
		t.Errorf("interspersed differs: after %q vs before %q", err.Error(), errBefore.Error())
	}
}

// TestDxAttachedShorthand checks -p8080:80 attached form parses.
func TestDxAttachedShorthand(t *testing.T) {
	_, _, errBad := dxRun([]string{"workload", "run", "-pinvalid", "nginx"})
	if errBad == nil {
		t.Fatal("-pinvalid: expected error, got nil")
	}
	dxMustContain(t, errBad.Error(), "invalid", "attached bad port")

	_, _, errEq := dxRun([]string{"workload", "run", "--port=invalid", "nginx"})
	if errEq == nil {
		t.Fatal("--port=invalid: expected error, got nil")
	}
	if !strings.Contains(errBad.Error(), "invalid") || !strings.Contains(errEq.Error(), "invalid") {
		t.Errorf("attached vs long differ: %q vs %q", errBad.Error(), errEq.Error())
	}

	_, _, errGood := dxRunWithEmptyDaemon(t, []string{"workload", "run", "-p8080:80", "nginx"})
	if errGood != nil && strings.Contains(errGood.Error(), "invalid") {
		t.Errorf("-p8080:80 should parse attached value, got %q", errGood.Error())
	}
}

// TestDxTerminatorStopsParsing checks -- ends flag parsing.
func TestDxTerminatorStopsParsing(t *testing.T) {
	t.Parallel()
	stdoutHelp, _, err := dxRun([]string{"workload", "inspect", "--help"})
	if err != nil {
		t.Fatalf("inspect help failed: %v", err)
	}
	if !strings.Contains(stdoutHelp, "concord workload inspect") {
		t.Fatalf("inspect help missing usage:\n%s", stdoutHelp)
	}

	_, _, errTerm := dxRun([]string{"workload", "inspect", "--", "--help"})
	if errTerm == nil {
		t.Fatal("inspect -- --help: expected error, got nil (terminator ignored)")
	}
	if strings.Contains(errTerm.Error(), "concord workload inspect") && strings.Contains(errTerm.Error(), "Display detailed specification") {
		t.Errorf("terminator treated --help as help flag: %v", errTerm)
	}
	// After -- the token is a positional id, so the error must be about
	// resolution or daemon, not about help or flag parsing.
	if strings.Contains(errTerm.Error(), "unknown flag") {
		t.Errorf("terminator token reported as flag: %v", errTerm)
	}
}

// TestDxBooleanFollow checks --follow and -f parse as booleans.
func TestDxBooleanFollow(t *testing.T) {
	_, _, err := dxRun([]string{"workload", "logs", "--follow"})
	if err == nil {
		t.Fatal("logs --follow without id: expected error, got nil")
	}
	if err.Error() != "accepts 1 arg(s), received 0" {
		t.Errorf("logs --follow no id = %q", err.Error())
	}
	_, _, errShort := dxRun([]string{"workload", "logs", "-f"})
	if errShort == nil {
		t.Fatal("logs -f without id: expected error, got nil")
	}
	if errShort.Error() != "accepts 1 arg(s), received 0" {
		t.Errorf("logs -f no id = %q", errShort.Error())
	}

	// Interspersed boolean after the id must still parse as a flag,
	// so arg count stays 1 and dispatch proceeds to the daemon layer.
	_, _, errAfter := dxRunWithEmptyDaemon(t, []string{"workload", "logs", "someid", "--follow"})
	if errAfter != nil && errAfter.Error() == "accepts 1 arg(s), received 0" {
		t.Errorf("interspersed --follow counted as positional: %v", errAfter)
	}
	if errAfter != nil && strings.Contains(errAfter.Error(), "unknown flag") {
		t.Errorf("interspersed --follow reported unknown: %v", errAfter)
	}
}

// TestDxRepeatableFlags checks --env accumulation and --file parsing.
func TestDxRepeatableFlags(t *testing.T) {
	_, _, err := dxRun([]string{"workload", "run", "--env", "A=1", "--env", "BAD", "nginx"})
	if err == nil {
		t.Fatal("repeatable env with bad entry: expected error, got nil")
	}
	dxMustContain(t, err.Error(), "invalid environment variable", "repeatable env")

	_, _, errEq := dxRun([]string{"workload", "run", "--env=A=1", "--env=B=2", "nginx"})
	_ = errEq
	// Both valid env entries must parse; without a daemon the call
	// proceeds past validation to the daemon layer, never a parse error.
	if errEq != nil && (strings.Contains(errEq.Error(), "invalid environment") || strings.Contains(errEq.Error(), "not defined")) {
		t.Errorf("valid repeatable env should parse, got %q", errEq.Error())
	}

	_, _, errFile := dxRunWithEmptyDaemon(t, []string{"workload", "run", "--file", "/a=/tmp/x", "--file", "/c=/tmp/y", "nginx"})
	if errFile != nil && strings.Contains(errFile.Error(), "not defined") {
		t.Errorf("repeatable file should parse, got %q", errFile.Error())
	}
	if errFile != nil && strings.Contains(errFile.Error(), "requires at least 1 arg") {
		t.Errorf("repeatable file consumed image positional: %v", errFile)
	}
}

// TestDxRunHelpDocumentsTerminator checks run help documents -- verbatim passthrough.
func TestDxRunHelpDocumentsTerminator(t *testing.T) {
	t.Parallel()
	stdout, _, err := dxRun([]string{"workload", "run", "--help"})
	if err != nil {
		t.Fatalf("run help failed: %v", err)
	}
	dxMustContain(t, stdout, "--", "workload run help terminator")
	dxMustContain(t, stdout, "verbatim", "workload run help verbatim")
}

// TestDxRunDashCommandTerminator checks -- passes dash-leading command words verbatim.
func TestDxRunDashCommandTerminator(t *testing.T) {
	_, _, err := dxRun([]string{"workload", "run", "nginx", "-g", "daemon", "off"})
	if err == nil {
		t.Fatal("run nginx -g without --: expected flag error, got nil")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined: -g") {
		t.Errorf("run without -- = %q, want flag parse error for -g", err.Error())
	}

	_, _, errTerm := dxRunWithEmptyDaemon(t, []string{"workload", "run", "nginx", "--", "-g", "daemon", "off"})
	if errTerm == nil {
		t.Fatal("run nginx -- -g: expected daemon error, got nil")
	}
	if strings.Contains(errTerm.Error(), "flag provided") || strings.Contains(errTerm.Error(), "unknown flag") {
		t.Errorf("run with -- treated command as flag: %v", errTerm)
	}
	if !strings.Contains(errTerm.Error(), "daemon") && !strings.Contains(errTerm.Error(), "submit workload") {
		t.Errorf("run with -- did not reach daemon layer: %v", errTerm)
	}

	_, _, errRm := dxRunWithEmptyDaemon(t, []string{"workload", "run", "nginx", "--", "--rm"})
	if errRm == nil {
		t.Fatal("run nginx -- --rm: expected daemon error, got nil")
	}
	if strings.Contains(errRm.Error(), "flag provided") || strings.Contains(errRm.Error(), "unknown flag") {
		t.Errorf("run with -- treated --rm as flag: %v", errRm)
	}
}
