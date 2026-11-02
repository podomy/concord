// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"testing"

	"github.com/podomy/concord/internal/geo"
)

// anchorKnown delegates the zero check to IsUnknown: the gossip zero
// point stays unknown while real and off-planet points behave as before.
func TestAnchorKnownUsesIsUnknown(t *testing.T) {
	t.Parallel()

	if anchorKnown(geo.Point{Lat: 0, Lon: 0}) {
		t.Fatal("(0, 0) must not be known")
	}
	if !anchorKnown(geo.Point{Lat: 47.6, Lon: 8.9}) {
		t.Fatal("real fix must be known")
	}
	if anchorKnown(geo.Point{Lat: 91, Lon: 0}) {
		t.Fatal("out-of-planet point must not be known")
	}
}
