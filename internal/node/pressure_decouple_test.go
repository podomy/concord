// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"testing"
)

// A failed disk source does not fail the sample: memory and CPU still
// update while disk keeps its previous reading.
func TestSampleHostDecouplesDiskFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	s := NewSampler()
	got, err := s.SampleHost()
	if err != nil {
		t.Fatalf("partial sample failed: %v", err)
	}
	if got.Mem == 0 || got.Mem > 100 {
		t.Fatalf("mem = %d, want a live reading", got.Mem)
	}
	if got.Disk != 0 {
		t.Fatalf("disk = %d, want previous (0) on failed source", got.Disk)
	}
	if last := s.LastHost(); last != got {
		t.Fatalf("LastHost = %+v, want %+v", last, got)
	}
}

// Inconsistent counters saturate instead of wrapping: idle can never
// exceed total, so an inverted pair reports 0 busy, not 100 percent.
func TestCPUSaturatesOnInvertedCounters(t *testing.T) {
	t.Parallel()

	before := cpuTimes{idle: 90, total: 100}
	after := cpuTimes{idle: 100, total: 101}
	if got := cpuPercent(before, after); got != 0 {
		t.Fatalf("cpu = %d, want 0 saturated", got)
	}
}
