// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"net/netip"
	"slices"

	"github.com/podomy/concord/internal/geo"
	nodepackage "github.com/podomy/concord/internal/node"
)

// AnchorResolver returns configured rendezvous addresses as join
// candidates. With a known self position it orders nearest-first, so a
// truck dials its own depot instead of a far anchor that happens to sit
// first in the list; without one it keeps list order. Entries with
// out-of-planet coordinates sort last: they may still dial fine, their
// position just orders nothing. Empty anchors resolve empty, which keeps
// LAN-only fleets on mDNS alone.
//
// A (0, 0) self position counts as unknown and keeps list order: (0, 0)
// is the gossip encoding for no fix, not a real fix (see anchorKnown).
type AnchorResolver struct {
	// Self is this node's position, or nil when unknown.
	Self *geo.Point
	// Anchors is the configured rendezvous list from node config.
	Anchors []nodepackage.AnchorEntry
}

// anchorKnown reports whether p is a real position fix. The exact zero
// point is unknown, not Null Island: members without a fix gossip (0, 0),
// and no fleet anchors at the Gulf of Guinea buoy, so (0, 0) must never
// win nearest-first. Out-of-planet points are not known either; Valid
// already rejects those.
func anchorKnown(p geo.Point) bool {
	return p != (geo.Point{}) && p.Valid()
}

// Resolve returns anchor addresses without any I/O: anchors are
// provisioned, not discovered.
func (a AnchorResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	// Copy: sorting must not reorder the shared config list.
	entries := make([]nodepackage.AnchorEntry, len(a.Anchors))
	copy(entries, a.Anchors)
	if a.Self != nil && anchorKnown(*a.Self) {
		self := *a.Self
		slices.SortStableFunc(entries, func(x, y nodepackage.AnchorEntry) int {
			return cmp.Compare(anchorDistance(self, x), anchorDistance(self, y))
		})
	}

	addrs := make([]netip.AddrPort, 0, len(entries))
	for _, entry := range entries {
		if !entry.Addr.IsValid() {
			continue
		}
		addrs = append(addrs, entry.Addr)
	}
	return addrs, nil
}

// anchorDistance is the great-circle distance to an entry, or infinite
// when its coordinates are unusable. Infinite sorts last without dropping
// the address. A (0, 0) entry is unknown, not Null Island (see
// anchorKnown): it sorts with the invalid ones. When every entry is
// unknown the order is just config order, same as no self position.
func anchorDistance(self geo.Point, entry nodepackage.AnchorEntry) float64 {
	p, ok := entry.AnchorPoint()
	if !ok || !anchorKnown(p) {
		return math.Inf(1)
	}
	return geo.DistanceKM(self, p)
}
