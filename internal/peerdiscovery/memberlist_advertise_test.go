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
	got, err := ResolveAdvertise(bind, adv)
	if err != nil {
		t.Fatalf("resolve advertise: %v", err)
	}
	// Advertise IP from config; port always from bind.
	want := netip.MustParseAddrPort("10.0.0.5:7946")
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResolveAdvertiseConcreteBind(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("127.0.0.1:0")
	got, err := ResolveAdvertise(bind, netip.Addr{})
	if err != nil {
		t.Fatalf("resolve advertise: %v", err)
	}
	if got != bind {
		t.Fatalf("got %v, want %v", got, bind)
	}
}

// An explicitly configured unspecified address is an operator error, not
// auto-detect: it fails instead of publishing an undialable endpoint.
func TestResolveAdvertiseUnspecifiedHintRejected(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("0.0.0.0:7946")
	for _, hint := range []netip.Addr{
		netip.MustParseAddr("0.0.0.0"),
		netip.MustParseAddr("::"),
	} {
		if _, err := ResolveAdvertise(bind, hint); err == nil {
			t.Fatalf("expected error for unspecified hint %s", hint)
		}
	}
}

func TestSkipAdvertiseIface(t *testing.T) {
	t.Parallel()

	if !skipAdvertiseIface("cn0") {
		t.Fatal("cn0 must be skipped")
	}
	if !skipAdvertiseIface("wg-a1b2c3d4") {
		t.Fatal("wg-* must be skipped")
	}
	for _, name := range []string{"docker0", "docker_gwbridge", "vethabc123", "br-4f2c9a"} {
		if !skipAdvertiseIface(name) {
			t.Fatalf("%s must be skipped", name)
		}
	}
	if skipAdvertiseIface("eth0") {
		t.Fatal("eth0 must not be skipped")
	}
}

func TestOverlayPrefix(t *testing.T) {
	t.Parallel()

	if !OverlayPrefix.Contains(netip.MustParseAddr("10.0.0.1")) {
		t.Fatal("overlay must contain 10.0.0.1")
	}
	if OverlayPrefix.Contains(netip.MustParseAddr("192.168.100.1")) {
		t.Fatal("underlay must be outside overlay")
	}
}

func TestResolveAdvertiseUnspecifiedFindsInterface(t *testing.T) {
	t.Parallel()

	bind := netip.MustParseAddrPort("0.0.0.0:7946")
	got, err := ResolveAdvertise(bind, netip.Addr{})
	if err != nil {
		t.Fatalf("resolve advertise: %v", err)
	}
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
