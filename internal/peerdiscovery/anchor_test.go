// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"encoding/json"
	"math"
	"net/netip"
	"testing"

	"github.com/podomy/concord/internal/geo"
	nodepackage "github.com/podomy/concord/internal/node"
)

// Near anchor sorts before far anchor when self is known.
func TestAnchorResolverNearestFirst(t *testing.T) {
	t.Parallel()

	self := &geo.Point{Lat: 47.6, Lon: 8.9}
	far := nodepackage.AnchorEntry{Name: "far", Addr: netip.MustParseAddrPort("10.0.0.2:7946"), Lat: 0, Lon: 0}
	near := nodepackage.AnchorEntry{Name: "near", Addr: netip.MustParseAddrPort("10.0.0.1:7946"), Lat: 47.61, Lon: 8.91}
	r := AnchorResolver{Self: self, Anchors: []nodepackage.AnchorEntry{far, near}}

	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != near.Addr || got[1] != far.Addr {
		t.Fatalf("got %v", got)
	}
}

// Without self position the list order decides.
func TestAnchorResolverListOrderWithoutSelf(t *testing.T) {
	t.Parallel()

	far := nodepackage.AnchorEntry{Addr: netip.MustParseAddrPort("10.0.0.2:7946"), Lat: 0, Lon: 0}
	near := nodepackage.AnchorEntry{Addr: netip.MustParseAddrPort("10.0.0.1:7946"), Lat: 47.61, Lon: 8.91}
	r := AnchorResolver{Anchors: []nodepackage.AnchorEntry{far, near}}

	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != far.Addr || got[1] != near.Addr {
		t.Fatalf("got %v", got)
	}
}

// Invalid coordinates sort last without dropping the address; invalid
// addresses drop.
func TestAnchorResolverInvalidSortsLast(t *testing.T) {
	t.Parallel()

	self := &geo.Point{Lat: 47.6, Lon: 8.9}
	broken := nodepackage.AnchorEntry{Name: "broken", Addr: netip.MustParseAddrPort("10.0.0.9:7946"), Lat: 91, Lon: 0}
	near := nodepackage.AnchorEntry{Name: "near", Addr: netip.MustParseAddrPort("10.0.0.1:7946"), Lat: 47.61, Lon: 8.91}
	noAddr := nodepackage.AnchorEntry{Name: "noaddr", Lat: 47.61, Lon: 8.91}
	r := AnchorResolver{Self: self, Anchors: []nodepackage.AnchorEntry{broken, noAddr, near}}

	got, err := r.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != near.Addr || got[1] != broken.Addr {
		t.Fatalf("got %v", got)
	}
}

// Empty anchors resolve empty, keeping LAN-only fleets on mDNS alone.
func TestAnchorResolverEmpty(t *testing.T) {
	t.Parallel()

	got, err := AnchorResolver{}.Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

// Cancelled context fails.
func TestAnchorResolverCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := AnchorResolver{Anchors: []nodepackage.AnchorEntry{{Addr: netip.MustParseAddrPort("10.0.0.1:7946")}}}
	if _, err := r.Resolve(ctx); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

// Position lands in gossiped metadata next to the pressure trio.
func TestSetPosition(t *testing.T) {
	t.Parallel()

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{
			Lat: microToDeg(delegate.lat.Load()),
			Lon: microToDeg(delegate.lon.Load()),
		}
	}

	ms := &MemberService{delegate: delegate}

	ms.SetPosition(47.6, 8.9)
	metaBytes := delegate.NodeMeta(512)

	var meta NodeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatal(err)
	}
	// Microdegree storage: exact to 1e-6 degrees, about 11cm.
	if math.Abs(meta.Lat-47.6) > 1e-6 || math.Abs(meta.Lon-8.9) > 1e-6 {
		t.Fatalf("position = %v/%v", meta.Lat, meta.Lon)
	}
	if len(metaBytes) > 512 {
		t.Fatalf("metadata %d bytes exceeds gossip cap", len(metaBytes))
	}

	ms.SetPosition(91, 0)
	metaBytes = delegate.NodeMeta(512)
	var kept NodeMetadata
	if err := json.Unmarshal(metaBytes, &kept); err != nil {
		t.Fatal(err)
	}
	if math.Abs(kept.Lat-47.6) > 1e-6 || math.Abs(kept.Lon-8.9) > 1e-6 {
		t.Fatalf("invalid set should keep previous: %v/%v", kept.Lat, kept.Lon)
	}

	var nilService *MemberService
	nilService.SetPosition(1, 2)
}

// The anchor bit gossips alongside everything else.
func TestSetAnchor(t *testing.T) {
	t.Parallel()

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{Anchor: delegate.anchor.Load()}
	}

	ms := &MemberService{delegate: delegate}

	ms.SetAnchor(true)
	metaBytes := delegate.NodeMeta(512)

	var meta NodeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatal(err)
	}
	if !meta.Anchor {
		t.Fatal("expected anchor bit set")
	}

	var nilService *MemberService
	nilService.SetAnchor(true)
}
