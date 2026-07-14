// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"net"
	"net/netip"
	"testing"

	"github.com/hashicorp/mdns"
)

func TestEntryToAddrPortIPv4(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("10.0.0.7").To4(),
		Port:   7946,
	}
	got, ok := entryToAddrPort(entry)
	if !ok {
		t.Fatal("expected ok")
	}
	want := netip.MustParseAddrPort("10.0.0.7:7946")
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEntryToAddrPortNoAddress(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{Port: 7946}
	if _, ok := entryToAddrPort(entry); ok {
		t.Fatal("expected not ok")
	}
}

func TestEntryToAddrPortInvalidPort(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("10.0.0.7").To4(),
		Port:   -1,
	}
	if _, ok := entryToAddrPort(entry); ok {
		t.Fatal("expected not ok for invalid port")
	}
}
