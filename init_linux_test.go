// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"
)

// Only libcontainer's exact re-exec argv counts: anything else, even
// containing "init" elsewhere, runs the daemon or CLI.
func TestIsContainerInit(t *testing.T) {
	t.Parallel()

	if !isContainerInit([]string{"concord", "init"}) {
		t.Error("expected re-exec argv to count as container init")
	}
	for _, args := range [][]string{
		{"concord"},
		{"concord", "--anchor"},
		{"concord", "node", "list"},
		{"concord", "--init"},
	} {
		if isContainerInit(args) {
			t.Errorf("expected %v to run the daemon, not container init", args)
		}
	}
}
