// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// Overwrites past the cap count as dropped: backpressure made visible.
func TestLogRingDroppedCountsOverwrites(t *testing.T) {
	t.Parallel()

	r := newLogRing(LogStdout)
	if r.Dropped() != 0 {
		t.Fatalf("dropped = %d, want 0 fresh", r.Dropped())
	}
	for i := range logRingSize + 5 {
		if _, err := r.Write(fmt.Appendf(nil, "line %d\n", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.Dropped(); got != 5 {
		t.Fatalf("dropped = %d, want 5", got)
	}
	if got := len(r.lines()); got != logRingSize {
		t.Fatalf("lines = %d, want cap %d", got, logRingSize)
	}
}

// Sampler totals dropped lines across both streams of all workloads,
// and forgets them with the workload.
func TestSamplerLogDroppedTotals(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	out := s.LogWriter(id, LogStdout)
	for i := range logRingSize + 3 {
		if _, err := out.Write(fmt.Appendf(nil, "line %d\n", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.LogDropped(); got != 3 {
		t.Fatalf("dropped = %d, want 3", got)
	}
	s.DropWorkload(id)
	if got := s.LogDropped(); got != 0 {
		t.Fatalf("dropped after drop = %d, want 0", got)
	}
}
