// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/concord/internal/geo"
)

// Appends land one JSON line per gated point with a precise timestamp;
// the file holds exactly what the ring holds, in the same order.
func TestTrackLogAppendsGatedPoints(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	track, err := OpenTrackLog()
	if err != nil {
		t.Fatal(err)
	}

	s := NewSampler()
	s.SetTrackLog(track)

	home := geo.Point{Lat: 47.6, Lon: 8.9}
	s.RecordPosition(home)
	s.RecordPosition(geo.Point{Lat: 47.60001, Lon: 8.9})
	away := geo.Point{Lat: 47.61, Lon: 8.91}
	s.RecordPosition(away)

	if err := track.Close(); err != nil {
		t.Fatal(err)
	}

	lines := readTrackLines(t, dir)
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
	for i, want := range []geo.Point{home, away} {
		assertTrackLine(t, lines[i], want)
	}
	if ring := s.Trail(); len(ring) != 2 || ring[0].Pos != home || ring[1].Pos != away {
		t.Fatalf("ring diverged from file: %+v", ring)
	}
}

// readTrackLines returns the raw lines of the temp dir's track file.
func readTrackLines(t *testing.T, dir string) []string {
	t.Helper()

	// #nosec G304: path comes from the test's own temp dir.
	raw, err := os.ReadFile(filepath.Join(dir, "concord", "track.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// assertTrackLine checks one track file line against its point.
func assertTrackLine(t *testing.T, line string, want geo.Point) {
	t.Helper()

	var got struct {
		At  time.Time `json:"At"`
		Lat float64   `json:"Lat"`
		Lon float64   `json:"Lon"`
	}
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatal(err)
	}
	if got.At.IsZero() || got.Lat != want.Lat || got.Lon != want.Lon {
		t.Fatalf("line = %+v, want %+v", got, want)
	}
}

// Without a sink the sampler keeps memory only and never touches disk.
func TestTrackLogNilKeepsMemoryOnly(t *testing.T) {
	s := NewSampler()
	s.RecordPosition(geo.Point{Lat: 47.6, Lon: 8.9})
	if len(s.Trail()) != 1 {
		t.Fatalf("trail = %+v", s.Trail())
	}
}
