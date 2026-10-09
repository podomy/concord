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
type AnchorResolver struct {
	// Self is this node's position, or nil when unknown.
	Self *geo.Point
	// Anchors is the configured rendezvous list from node config.
	Anchors []nodepackage.AnchorEntry
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
	if a.Self != nil && a.Self.Valid() {
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
// the address.
func anchorDistance(self geo.Point, entry nodepackage.AnchorEntry) float64 {
	p, ok := entry.AnchorPoint()
	if !ok {
		return math.Inf(1)
	}
	return geo.DistanceKM(self, p)
}
