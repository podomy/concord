// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"
)

// Bare invocation means a plain daemon start.
func TestParseDaemonFlagsEmpty(t *testing.T) {
	t.Parallel()

	got, err := parseDaemonFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("expected no anchor flag")
	}
}

// --anchor marks the node for anchor provisioning.
func TestParseDaemonFlagsAnchor(t *testing.T) {
	t.Parallel()

	got, err := parseDaemonFlags([]string{"--anchor"})
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("expected anchor flag")
	}
}

// Unknown flags and positional args fail.
func TestParseDaemonFlagsInvalid(t *testing.T) {
	t.Parallel()

	if _, err := parseDaemonFlags([]string{"--nope"}); err == nil {
		t.Fatal("expected error for unknown flag")
	}
	if _, err := parseDaemonFlags([]string{"workload"}); err == nil {
		t.Fatal("expected error for positional args")
	}
}
