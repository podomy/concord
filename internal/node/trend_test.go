// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A ring past capacity keeps the newest window: the average converges on
// recent values, not lifetime ones.
func TestTrendOverwriteKeepsNewest(t *testing.T) {
	t.Parallel()

	r := newTrend()
	for range trendWindow {
		r.add(trendPoint{cpu: 0, mem: 0})
	}
	for range 10 {
		r.add(trendPoint{cpu: 100, mem: 100})
	}
	if got := r.len(); got != trendWindow {
		t.Fatalf("len = %d, want %d", got, trendWindow)
	}
	cpu, mem, n := r.average()
	if n != trendWindow {
		t.Fatalf("n = %d, want %d", n, trendWindow)
	}
	// 10 hot trendPoints in a 720 window: mean rounds down near zero, but the
	// window is the newest 720, proven by exact count below.
	if cpu != uint8(100*10/trendWindow) || mem != uint8(100*10/trendWindow) {
		t.Fatalf("avg = %d/%d", cpu, mem)
	}
}

// Exact means over a small window.
func TestTrendAverage(t *testing.T) {
	t.Parallel()

	r := newTrend()
	r.add(trendPoint{cpu: 20, mem: 40})
	r.add(trendPoint{cpu: 40, mem: 60})
	cpu, mem, n := r.average()
	if cpu != 30 || mem != 50 || n != 2 {
		t.Fatalf("avg = %d/%d/%d", cpu, mem, n)
	}
}

// Empty rings report zeros.
func TestTrendAverageEmpty(t *testing.T) {
	t.Parallel()

	cpu, mem, n := newTrend().average()
	if cpu != 0 || mem != 0 || n != 0 {
		t.Fatalf("avg = %d/%d/%d", cpu, mem, n)
	}
}

// Capture snapshots current readings; Trend folds them. Unknown ids and
// dropped workloads report zeros.
func TestCaptureTrend(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	t0 := time.Now()
	s.SampleWorkload(id, 1_000_000_000, 256*1024*1024, 1024*1024*1024, t0)
	s.SampleWorkload(id, 1_500_000_000, 256*1024*1024, 1024*1024*1024, t0.Add(time.Second))
	s.CaptureTrend()
	s.SampleWorkload(id, 2_500_000_000, 256*1024*1024, 1024*1024*1024, t0.Add(2*time.Second))
	s.CaptureTrend()

	cpu, mem, n := s.Trend(id)
	if n != 2 {
		t.Fatalf("n = %d, want 2", n)
	}
	// First capture saw CPU 50 (500ms over 1s), second saw 100 (1s over 1s):
	// mean 75. Memory steady 25.
	if cpu != 75 || mem != 25 {
		t.Fatalf("trend = %d/%d", cpu, mem)
	}

	if c, m, n := s.Trend(uuid.New()); c != 0 || m != 0 || n != 0 {
		t.Fatalf("unknown = %d/%d/%d", c, m, n)
	}

	s.DropWorkload(id)
	if c, m, n := s.Trend(id); c != 0 || m != 0 || n != 0 {
		t.Fatalf("dropped = %d/%d/%d", c, m, n)
	}
}
