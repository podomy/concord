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

// TestCLIHelpGolden pins the root and workload run help text.
func TestCLIHelpGolden(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(context.Background(), []string{"--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("root help failed: %v", err)
	}
	root := stdout.String()
	for _, want := range []string{
		"coordination layer for distributed systems",
		"Available Commands",
		"workload",
		"node",
		"metrics",
		"-h, --help",
	} {
		if !strings.Contains(root, want) {
			t.Errorf("root help missing %q:\n%s", want, root)
		}
	}

	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(context.Background(), []string{"-h"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("root -h failed: %v", err)
	}
	if stdout.String() != root {
		t.Errorf("root -h differs from --help:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(context.Background(), []string{"workload", "run", "--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload run help failed: %v", err)
	}
	run := stdout.String()
	for _, want := range []string{
		"run [flags] <image>",
		"--port",
		"-p",
		"--env",
		"-e",
		"--restart",
		"always",
		"--cpu",
		"1024",
		"--memory",
		"--health-path",
		"--health-action",
		"--health-probe",
		"--health-cmd",
		"--depends-on",
		"--qos",
		"besteffort",
		"--file",
	} {
		if !strings.Contains(run, want) {
			t.Errorf("workload run help missing %q:\n%s", want, run)
		}
	}
}
