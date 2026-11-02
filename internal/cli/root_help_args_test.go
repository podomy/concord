// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli_test

import (
	"strings"
	"testing"
)

// concord --help <path> routes through dispatch: workload help for a
// group, leaf help for a leaf, and an error (not root help) for an
// unknown subcommand. Bare --help behavior is unchanged.
func TestDxRootHelpWithArgs(t *testing.T) {
	t.Parallel()

	bare, _, err := dxRun([]string{"--help"})
	if err != nil {
		t.Fatalf("bare --help failed: %v", err)
	}
	if !strings.Contains(bare, "Available Commands") {
		t.Fatalf("bare --help missing commands:\n%s", bare)
	}

	workload, _, err := dxRun([]string{"--help", "workload"})
	if err != nil {
		t.Fatalf("--help workload failed: %v", err)
	}
	if !strings.Contains(workload, "Manage container workloads") {
		t.Fatalf("--help workload missing group help:\n%s", workload)
	}

	leaf, _, err := dxRun([]string{"--help", "workload", "run"})
	if err != nil {
		t.Fatalf("--help workload run failed: %v", err)
	}
	if !strings.Contains(leaf, "concord workload run") {
		t.Fatalf("--help workload run missing leaf help:\n%s", leaf)
	}

	_, _, err = dxRun([]string{"--help", "frobnicate"})
	if err == nil {
		t.Fatal("--help frobnicate: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("--help frobnicate = %v, want unknown command", err)
	}
}

// -help mirrors --help everywhere -h does: root and every leaf.
func TestDxDashHelpParity(t *testing.T) {
	t.Parallel()

	dash, _, err := dxRun([]string{"-help"})
	if err != nil {
		t.Fatalf("-help failed: %v", err)
	}
	long, _, err := dxRun([]string{"--help"})
	if err != nil {
		t.Fatalf("--help failed: %v", err)
	}
	if dash != long {
		t.Errorf("-help differs from --help:\n--- -help ---\n%s\n--- --help ---\n%s", dash, long)
	}

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
			dashOut, _, err := dxRun(append(args, "-help"))
			if err != nil {
				t.Fatalf("%v -help failed: %v", args, err)
			}
			longOut, _, err := dxRun(append(args, "--help"))
			if err != nil {
				t.Fatalf("%v --help failed: %v", args, err)
			}
			if dashOut != longOut {
				t.Errorf("%v: -help differs from --help", args)
			}
		})
	}
}
