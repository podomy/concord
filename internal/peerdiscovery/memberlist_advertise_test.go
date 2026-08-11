// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"net/netip"
	"testing"
)

func TestResolveAdvertiseExplicit(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("0.0.0.0:7946")
	adv := netip.MustParseAddr("10.0.0.5")
	got := ResolveAdvertise(bind, adv)
	// Advertise IP from config; port always from bind.
	want := netip.MustParseAddrPort("10.0.0.5:7946")
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResolveAdvertiseConcreteBind(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("127.0.0.1:0")
	got := ResolveAdvertise(bind, netip.Addr{})
	if got != bind {
		t.Fatalf("got %v, want %v", got, bind)
	}
}

func TestResolveAdvertiseUnspecifiedFindsInterface(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("0.0.0.0:7946")
	got := ResolveAdvertise(bind, netip.Addr{})
	if !got.IsValid() {
		// Possible on hosts with no non-loopback unicast addresses.
		t.Skip("no non-loopback interface address available")
	}
	if got.Addr().IsUnspecified() || got.Addr().IsLoopback() {
		t.Fatalf("got unusable advertise address %v", got)
	}
	if got.Port() != 7946 {
		t.Fatalf("port = %d, want 7946", got.Port())
	}
}
