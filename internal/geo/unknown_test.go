// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package geo

import (
	"math"
	"testing"
)

// Only the exact zero point is unknown: anything else, even (0, 1) or an
// off-planet fix, is a position (maybe invalid, never unknown). NaN is
// invalid, not unknown.
func TestIsUnknown(t *testing.T) {
	t.Parallel()

	if !(Point{}).IsUnknown() {
		t.Fatal("(0, 0) must be unknown")
	}
	for _, p := range []Point{
		{Lat: 0, Lon: 1},
		{Lat: 1, Lon: 0},
		{Lat: 47.6, Lon: 8.9},
		{Lat: 91, Lon: 0},
		{Lat: math.NaN(), Lon: 0},
		{Lat: 0, Lon: math.Inf(1)},
	} {
		if p.IsUnknown() {
			t.Fatalf("%+v must not be unknown", p)
		}
	}
}
