// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package geo

import (
	"math"
	"testing"
)

// Near-antipodal points stay finite and land just under half the
// circumference: float rounding must not push the haversine out of domain.
func TestDistanceNearAntipodal(t *testing.T) {
	t.Parallel()

	got := DistanceKM(Point{Lat: 0, Lon: 0}, Point{Lat: 0, Lon: 179.999999})
	if math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("got %v, want finite", got)
	}
	half := math.Pi * earthRadiusKM
	if got <= half-1 || got > half {
		t.Fatalf("got %v, want just under %v", got, half)
	}
}

// The exact antipode is covered, but near-antipodes from float-heavy inputs
// (tiny offsets off the pole) also stay finite.
func TestDistanceNearAntipodalOffPole(t *testing.T) {
	t.Parallel()

	got := DistanceKM(Point{Lat: 89.999999, Lon: 0}, Point{Lat: -89.999999, Lon: 180})
	if math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("got %v, want finite", got)
	}
}

// NaN inputs yield NaN, never a panic. Callers must Valid-check; the
// function only guarantees it cannot crash or return a bogus finite
// distance.
func TestDistanceNaNInput(t *testing.T) {
	t.Parallel()

	for _, p := range []Point{
		{Lat: math.NaN(), Lon: 0},
		{Lat: 0, Lon: math.NaN()},
		{Lat: math.NaN(), Lon: math.NaN()},
	} {
		if got := DistanceKM(p, Point{Lat: 0, Lon: 0}); !math.IsNaN(got) {
			t.Fatalf("got %v, want NaN for %+v", got, p)
		}
		if got := DistanceKM(Point{Lat: 0, Lon: 0}, p); !math.IsNaN(got) {
			t.Fatalf("got %v, want NaN for %+v", got, p)
		}
	}
}

// Infinite inputs yield NaN, never a panic or an infinite distance.
func TestDistanceInfInput(t *testing.T) {
	t.Parallel()

	for _, p := range []Point{
		{Lat: math.Inf(1), Lon: 0},
		{Lat: 0, Lon: math.Inf(-1)},
	} {
		if got := DistanceKM(p, Point{Lat: 0, Lon: 0}); !math.IsNaN(got) {
			t.Fatalf("got %v, want NaN for %+v", got, p)
		}
	}
}
