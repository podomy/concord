// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package geo

import (
	"math"
	"testing"
)

// Same point measures zero.
func TestDistanceSamePoint(t *testing.T) {
	t.Parallel()

	p := Point{Lat: 47.6, Lon: 8.9}
	if got := DistanceKM(p, p); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

// One degree of longitude at the equator is about 111.19km.
func TestDistanceEquatorDegree(t *testing.T) {
	t.Parallel()

	got := DistanceKM(Point{Lat: 0, Lon: 0}, Point{Lat: 0, Lon: 1})
	if math.Abs(got-111.19) > 0.01 {
		t.Fatalf("got %v, want ~111.19", got)
	}
}

// Antipodes sit half the circumference apart.
func TestDistanceAntipodes(t *testing.T) {
	t.Parallel()

	got := DistanceKM(Point{Lat: 0, Lon: 0}, Point{Lat: 0, Lon: 180})
	if math.Abs(got-20015.09) > 0.1 {
		t.Fatalf("got %v, want ~20015.09", got)
	}
}

// Poles converge: one longitude degree near the pole is short.
func TestDistanceNearPole(t *testing.T) {
	t.Parallel()

	got := DistanceKM(Point{Lat: 89, Lon: 0}, Point{Lat: 89, Lon: 1})
	if got >= 111.19 || got <= 0 {
		t.Fatalf("got %v, want (0, 111.19)", got)
	}
	// About 1.95km at 89 degrees.
	if math.Abs(got-1.95) > 0.05 {
		t.Fatalf("got %v, want ~1.95", got)
	}
}

// Symmetry: direction never matters.
func TestDistanceSymmetric(t *testing.T) {
	t.Parallel()

	a := Point{Lat: 47.6, Lon: 8.9}
	b := Point{Lat: -23.5, Lon: 133.7}
	if DistanceKM(a, b) != DistanceKM(b, a) {
		t.Fatal("distance is not symmetric")
	}
}

// Range edges hold, outside fails.
func TestPointValid(t *testing.T) {
	t.Parallel()

	for _, p := range []Point{{Lat: 0, Lon: 0}, {Lat: -90, Lon: -180}, {Lat: 90, Lon: 180}} {
		if !p.Valid() {
			t.Fatalf("%+v should be valid", p)
		}
	}
	for _, p := range []Point{{Lat: -90.1, Lon: 0}, {Lat: 0, Lon: 180.1}, {Lat: 91, Lon: 0}} {
		if p.Valid() {
			t.Fatalf("%+v should be invalid", p)
		}
	}
}
