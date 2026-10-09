// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"net/netip"
	"testing"
)

func TestResolverFunc(t *testing.T) {
	t.Parallel()

	want := netip.MustParseAddrPort("192.0.2.1:1")
	f := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return []netip.AddrPort{want}, nil
	})
	got, err := f.Resolve(context.Background())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %v, want [%v]", got, want)
	}
}

func TestAddrPortsToStrings(t *testing.T) {
	t.Parallel()

	addrs := []netip.AddrPort{
		netip.MustParseAddrPort("10.0.0.1:7946"),
		netip.MustParseAddrPort("[::1]:7946"),
	}
	got := addrPortsToStrings(addrs)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if got[0] != "10.0.0.1:7946" {
		t.Fatalf("got %q", got[0])
	}
	if got[1] != "[::1]:7946" {
		t.Fatalf("got %q", got[1])
	}
}
