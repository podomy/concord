// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/geo"
)

// First host sample establishes the CPU baseline: CPU 0, memory and disk
// reported immediately within 0-100.
func TestSampleHostFirstCall(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "concord"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", dir)

	s := NewSampler()
	got, err := s.SampleHost()
	if err != nil {
		t.Fatal(err)
	}
	if got.CPU != 0 {
		t.Fatalf("first CPU = %d, want 0 (baseline)", got.CPU)
	}
	if got.Mem > 100 || got.Disk > 100 {
		t.Fatalf("out of range: %+v", got)
	}
	if last := s.LastHost(); last != got {
		t.Fatalf("LastHost = %+v, want %+v", last, got)
	}
}

// Second sample carries a CPU delta in 0-100.
func TestSampleHostSecondCall(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "concord"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", dir)

	s := NewSampler()
	if _, err := s.SampleHost(); err != nil {
		t.Fatal(err)
	}
	// Burn some user time so the delta is non-trivial.
	burnUntil := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(burnUntil) {
	}
	got, err := s.SampleHost()
	if err != nil {
		t.Fatal(err)
	}
	if got.CPU > 100 {
		t.Fatalf("CPU = %d", got.CPU)
	}
}

// Fixed deltas give exact percents: 250ms of CPU over 1s is 25 percent,
// 256MB of 1024MB is 25 percent.
func TestSampleWorkloadMath(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	t0 := time.Now()
	first := s.SampleWorkload(id, 1_000_000_000, 256*1024*1024, 1024*1024*1024, t0)
	if first.CPUPercent != 0 {
		t.Fatalf("first CPU = %d, want 0 (no delta)", first.CPUPercent)
	}
	if first.MemPercent != 25 || first.MemUsageMB != 256 {
		t.Fatalf("first = %+v", first)
	}
	second := s.SampleWorkload(id, 1_250_000_000, 512*1024*1024, 1024*1024*1024, t0.Add(time.Second))
	if second.CPUPercent != 25 {
		t.Fatalf("CPU = %d, want 25", second.CPUPercent)
	}
	if second.MemPercent != 50 || second.MemUsageMB != 512 {
		t.Fatalf("second = %+v", second)
	}
}

// A backwards counter (cgroup recreated) reports 0 instead of wrapping,
// and unlimited memory reports usage without a percent.
func TestSampleWorkloadResetAndUnlimited(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	t0 := time.Now()
	s.SampleWorkload(id, 5_000_000_000, 100*1024*1024, 0, t0)
	got := s.SampleWorkload(id, 100_000_000, 100*1024*1024, 0, t0.Add(time.Second))
	if got.CPUPercent != 0 {
		t.Fatalf("CPU after reset = %d, want 0", got.CPUPercent)
	}
	if got.MemPercent != 0 || got.MemUsageMB != 100 {
		t.Fatalf("unlimited = %+v", got)
	}
}

// Zero elapsed time cannot divide: reports 0.
func TestSampleWorkloadZeroElapsed(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	t0 := time.Now()
	s.SampleWorkload(id, 1_000_000_000, 0, 0, t0)
	got := s.SampleWorkload(id, 2_000_000_000, 0, 0, t0)
	if got.CPUPercent != 0 {
		t.Fatalf("CPU = %d, want 0", got.CPUPercent)
	}
}

// Dropped ids disappear from both counters and samples.
func TestDropWorkload(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	t0 := time.Now()
	s.SampleWorkload(id, 1_000_000_000, 0, 0, t0)
	s.DropWorkload(id)
	if len(s.Workloads()) != 0 {
		t.Fatal("expected no samples after drop")
	}
	// Sampling again starts a fresh baseline, not a delta against dropped state.
	got := s.SampleWorkload(id, 9_000_000_000, 0, 0, t0.Add(time.Second))
	if got.CPUPercent != 0 {
		t.Fatalf("CPU = %d, want 0", got.CPUPercent)
	}
}

// First valid position records; sub-10m moves do not; real roams append
// oldest-first; invalid positions and the zero unknown-point never record.
func TestRecordPositionTrail(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	home := geo.Point{Lat: 47.6, Lon: 8.9}
	s.RecordPosition(home)
	s.RecordPosition(geo.Point{Lat: 47.60001, Lon: 8.9})
	s.RecordPosition(geo.Point{Lat: 91, Lon: 0})
	s.RecordPosition(geo.Point{Lat: 0, Lon: 0})
	away := geo.Point{Lat: 47.61, Lon: 8.91}
	s.RecordPosition(away)

	trail := s.Trail()
	if len(trail) != 2 {
		t.Fatalf("len = %d, want 2", len(trail))
	}
	if trail[0].Pos != home || trail[1].Pos != away {
		t.Fatalf("trail = %+v", trail)
	}
}
