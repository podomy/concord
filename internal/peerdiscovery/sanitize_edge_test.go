// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"math"
	"testing"
)

// Non-finite remote positions sanitize to unknown (0, 0): NaN and Inf
// fail Valid, so they reset like any off-planet coordinate instead of
// propagating into anchor math.
func TestSanitizeNodeMetaNonFinite(t *testing.T) {
	t.Parallel()

	for _, p := range [][2]float64{
		{math.NaN(), 0},
		{0, math.NaN()},
		{math.Inf(1), 0},
		{0, math.Inf(-1)},
	} {
		got := sanitizeNodeMeta(NodeMetadata{Lat: p[0], Lon: p[1]})
		if got.Lat != 0 || got.Lon != 0 {
			t.Fatalf("position = %v/%v, want 0/0", got.Lat, got.Lon)
		}
	}
}
