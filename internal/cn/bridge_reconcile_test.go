// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"testing"
)

// Stale 10.0.x.1/24 addrs that do not match current drop; everything else
// stays. EEXIST tolerance lives in CreateBridge, not here.
func TestStaleBridgeAddrs(t *testing.T) {
	t.Parallel()

	have := []string{"10.0.1.1/24", "10.0.2.1/24", "10.0.2.5/24", "192.168.1.1/24", "10.0.3.1/32"}
	got := staleBridgeAddrs(have, "10.0.2.1/24")
	if len(got) != 1 || got[0] != "10.0.1.1/24" {
		t.Fatalf("stale = %v, want [10.0.1.1/24]", got)
	}
}

// Current addr never stale, even alone.
func TestStaleBridgeAddrsKeepsCurrent(t *testing.T) {
	t.Parallel()

	got := staleBridgeAddrs([]string{"10.0.5.1/24"}, "10.0.5.1/24")
	if len(got) != 0 {
		t.Fatalf("stale = %v, want []", got)
	}
}
