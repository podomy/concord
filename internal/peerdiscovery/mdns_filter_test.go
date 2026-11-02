// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"net"
	"net/netip"
	"testing"

	"github.com/hashicorp/mdns"
)

// A plain LAN address with a valid port passes and maps to the same
// endpoint.
func TestEntryToAddrPortExValidIPv4(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("192.168.1.7").To4(),
		Port:   7946,
	}
	got, ok := entryToAddrPortEx(entry, netip.AddrPort{})
	if !ok {
		t.Fatal("expected ok")
	}
	want := netip.MustParseAddrPort("192.168.1.7:7946")
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// Global IPv6 passes through.
func TestEntryToAddrPortExValidIPv6(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV6IPAddr: &net.IPAddr{IP: net.ParseIP("2001:db8::1")},
		Port:         7946,
	}
	got, ok := entryToAddrPortEx(entry, netip.AddrPort{})
	if !ok {
		t.Fatal("expected ok")
	}
	want := netip.MustParseAddrPort("[2001:db8::1]:7946")
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// IPv4-mapped IPv6 unmaps to plain IPv4.
func TestEntryToAddrPortExMappedIPv4(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("192.168.1.7").To4(),
		Port:   7946,
	}
	got, ok := entryToAddrPortEx(entry, netip.AddrPort{})
	if !ok {
		t.Fatal("expected ok")
	}
	if !got.Addr().Is4() {
		t.Fatalf("got %v, want plain IPv4", got)
	}
}

// Loopback, link-local, unspecified, and multicast never become Join
// candidates.
func TestEntryToAddrPortExDropsUnusable(t *testing.T) {
	t.Parallel()

	ips := []string{
		"127.0.0.1",
		"169.254.1.2",
		"0.0.0.0",
		"224.0.0.251",
		"::1",
		"fe80::1",
		"::",
		"ff02::1",
	}
	for _, s := range ips {
		ip := net.ParseIP(s)
		entry := &mdns.ServiceEntry{Port: 7946}
		if ip.To4() != nil {
			entry.AddrV4 = ip.To4()
		} else {
			entry.AddrV6IPAddr = &net.IPAddr{IP: ip}
		}
		if _, ok := entryToAddrPortEx(entry, netip.AddrPort{}); ok {
			t.Fatalf("%s should not pass", s)
		}
	}
}

// Overlay-range addresses leak a local bridge and must not Join.
func TestEntryToAddrPortExDropsOverlay(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("10.0.0.7").To4(),
		Port:   7946,
	}
	if _, ok := entryToAddrPortEx(entry, netip.AddrPort{}); ok {
		t.Fatal("overlay address should not pass")
	}
}

// Zoned IPv6 drops: netip discards the zone, and a zone-scoped address
// without its zone is undialable.
func TestEntryToAddrPortExDropsZonedIPv6(t *testing.T) {
	t.Parallel()

	entry := &mdns.ServiceEntry{
		AddrV6IPAddr: &net.IPAddr{IP: net.ParseIP("fe80::1"), Zone: "eth0"},
		Port:         7946,
	}
	if _, ok := entryToAddrPortEx(entry, netip.AddrPort{}); ok {
		t.Fatal("zoned IPv6 should not pass")
	}
}

// Self addresses drop so a node never joins itself; zero Self disables
// only that check.
func TestEntryToAddrPortExDropsSelf(t *testing.T) {
	t.Parallel()

	self := netip.MustParseAddrPort("192.168.1.7:7946")
	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("192.168.1.7").To4(),
		Port:   7946,
	}
	if _, ok := entryToAddrPortEx(entry, self); ok {
		t.Fatal("self address should not pass")
	}
	if _, ok := entryToAddrPortEx(entry, netip.AddrPort{}); !ok {
		t.Fatal("zero self should not filter")
	}
}

// Missing addresses and out of range ports still fail.
func TestEntryToAddrPortExBadInput(t *testing.T) {
	t.Parallel()

	if _, ok := entryToAddrPortEx(&mdns.ServiceEntry{Port: 7946}, netip.AddrPort{}); ok {
		t.Fatal("expected not ok without address")
	}
	entry := &mdns.ServiceEntry{
		AddrV4: net.ParseIP("192.168.1.7").To4(),
		Port:   65536,
	}
	if _, ok := entryToAddrPortEx(entry, netip.AddrPort{}); ok {
		t.Fatal("expected not ok for out of range port")
	}
}

// The insert cap bounds the collection itself: past 1024 entries the
// rest drop and retry next round instead of growing memory.
func TestCollectEntriesCapsAtInsert(t *testing.T) {
	t.Parallel()

	ch := make(chan *mdns.ServiceEntry, maxMDNSEntries+64)
	go func() {
		for range maxMDNSEntries + 6 {
			ch <- &mdns.ServiceEntry{
				AddrV4: net.ParseIP("192.168.1.7").To4(),
				Port:   7946,
			}
		}
		close(ch)
	}()

	entries, dropped := collectEntries(ch)
	if len(entries) != maxMDNSEntries {
		t.Fatalf("entries = %d, want %d", len(entries), maxMDNSEntries)
	}
	if dropped != 6 {
		t.Fatalf("dropped = %d, want 6", dropped)
	}
}
