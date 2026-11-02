// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/hashicorp/mdns"

	"github.com/podomy/concord/internal/logs"
)

// maxMDNSEntries caps how many discovered entries one Resolve round
// processes. mDNS answers are unauthenticated LAN traffic, so an unbounded
// round lets one noisy network stall Join; past the cap the rest are
// dropped with a debug note and retried next round.
const maxMDNSEntries = 1024

// MDNSResolver discovers Concord peers on the local network over mDNS.
type MDNSResolver struct {
	Timeout time.Duration
	// Self is this node's memberlist endpoint. Entries resolving to Self
	// are dropped so a node never joins itself. Zero value disables the
	// check; callers that already filter self addresses (runtime does in
	// filterJoinCandidates) may leave it unset.
	Self netip.AddrPort
	// Logger receives debug notes such as the entry cap. Nil disables
	// logging.
	Logger *logs.Logger
}

// Resolve discovers Concord peers on the local network using multicast DNS.
// It browses for the configured mDNS service and returns discovered peer addresses.
func (m *MDNSResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	// Browse the local network for the concord nodes.
	serviceEntries, err := m.mdnsBrowse(ctx)
	if err != nil {
		return nil, fmt.Errorf("browsing the local network: %w", err)
	}

	var addresses []netip.AddrPort
	for i, entry := range serviceEntries {
		if i >= maxMDNSEntries {
			if m.Logger != nil {
				m.Logger.Debug(
					"mdns entry cap reached, rest dropped until next round",
					logs.Int("cap", maxMDNSEntries),
					logs.Int("total", len(serviceEntries)),
				)
			}
			break
		}
		addr, ok := entryToAddrPortEx(entry, m.Self)
		if !ok {
			continue
		}
		addresses = append(addresses, addr)
	}

	return addresses, nil
}

// entryToAddrPort extracts a netip.AddrPort from an mDNS service entry.
// It returns false if the entry has no valid address or port.
//
// Deprecated: frozen for the pre-filtering behavior its tests pin. New
// callers use entryToAddrPortEx, which additionally drops loopback,
// link-local, unspecified, multicast, zoned IPv6, overlay-range, and self
// addresses. Resolve uses the Ex form.
func entryToAddrPort(entry *mdns.ServiceEntry) (netip.AddrPort, bool) {
	var ip net.IP
	switch {
	case entry.AddrV4 != nil:
		ip = entry.AddrV4
	case entry.AddrV6IPAddr != nil:
		ip = entry.AddrV6IPAddr.IP
	default:
		return netip.AddrPort{}, false
	}

	if entry.Port < 0 || entry.Port > 65535 {
		return netip.AddrPort{}, false
	}

	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.AddrPort{}, false
	}

	return netip.AddrPortFrom(addr, uint16(entry.Port)), true
}

// entryToAddrPortEx extracts a dialable netip.AddrPort from an mDNS service
// entry. It returns false for anything peers must not Join: missing or out
// of range ports, undecodable IPs, loopback, link-local, unspecified,
// multicast, zoned IPv6, and our overlay range (mDNS must carry underlay
// addresses; an overlay address leaks a local bridge). A valid self drops
// too so a node never joins itself.
func entryToAddrPortEx(entry *mdns.ServiceEntry, self netip.AddrPort) (netip.AddrPort, bool) {
	ip, ok := mdnsEntryIP(entry)
	if !ok {
		return netip.AddrPort{}, false
	}

	if entry.Port < 0 || entry.Port > 65535 {
		return netip.AddrPort{}, false
	}

	addr, ok := usableAddr(ip)
	if !ok {
		return netip.AddrPort{}, false
	}

	out := netip.AddrPortFrom(addr, uint16(entry.Port))
	if self.IsValid() && out == self {
		return netip.AddrPort{}, false
	}
	return out, true
}

// mdnsEntryIP prefers the entry's IPv4 address and falls back to IPv6.
// Zoned IPv6 drops: netip conversion discards the zone, and a zone-scoped
// address without its zone is undialable.
func mdnsEntryIP(entry *mdns.ServiceEntry) (net.IP, bool) {
	if entry.AddrV4 != nil {
		return entry.AddrV4, true
	}
	if entry.AddrV6IPAddr != nil {
		if entry.AddrV6IPAddr.Zone != "" {
			return nil, false
		}
		if entry.AddrV6IPAddr.IP == nil {
			return nil, false
		}
		return entry.AddrV6IPAddr.IP, true
	}
	return nil, false
}

// usableAddr normalizes a discovered IP and reports whether peers may dial
// it. Unmap turns IPv4-mapped IPv6 (::ffff:a.b.c.d) into plain IPv4 so the
// result compares equal with underlay addresses elsewhere. Loopback,
// link-local, unspecified, multicast, and our overlay range never become
// Join candidates.
func usableAddr(ip net.IP) (netip.Addr, bool) {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}
	addr = addr.Unmap()
	if !addr.IsValid() || addr.IsLoopback() || addr.IsLinkLocalUnicast() ||
		addr.IsUnspecified() || addr.IsMulticast() {
		return netip.Addr{}, false
	}
	if OverlayPrefix.Contains(addr) {
		return netip.Addr{}, false
	}
	return addr, true
}

// mdnsBrowse sends a multicast mDNS query for the Concord service and returns
// all discovered service entries found within the resolver's timeout.
func (m *MDNSResolver) mdnsBrowse(ctx context.Context) ([]*mdns.ServiceEntry, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	entriesCh := make(chan *mdns.ServiceEntry, 64)

	var entries []*mdns.ServiceEntry
	var dropped int
	var wg sync.WaitGroup

	wg.Go(func() {
		entries, dropped = collectEntries(entriesCh)
	})

	// mdns.Query sends a single multicast query and collects responses
	// until the configured timeout, then returns. It is not indefinite.
	params := mdns.DefaultParams(DNSService)
	params.Timeout = m.Timeout
	if params.Timeout == 0 {
		params.Timeout = time.Second
	}
	params.Entries = entriesCh
	err := mdns.Query(params)
	close(entriesCh)

	wg.Wait()

	if err != nil {
		return nil, fmt.Errorf("mdns lookup: %w", err)
	}

	if dropped > 0 && m.Logger != nil {
		m.Logger.Debug(
			"mdns browse cap reached at insert, rest dropped until next round",
			logs.Int("cap", maxMDNSEntries),
			logs.Int("dropped", dropped),
		)
	}

	return entries, nil
}

// collectEntries drains ch until closed, keeping at most maxMDNSEntries
// and counting the rest as dropped. The cap sits at insert because mDNS
// answers are unauthenticated LAN traffic: bounding the collection itself
// keeps one noisy round from growing memory before processing starts.
// Resolve keeps its own processing cap as a second net.
func collectEntries(ch <-chan *mdns.ServiceEntry) ([]*mdns.ServiceEntry, int) {
	var entries []*mdns.ServiceEntry
	dropped := 0
	for entry := range ch {
		if len(entries) >= maxMDNSEntries {
			dropped++
			continue
		}
		entries = append(entries, entry)
	}
	return entries, dropped
}
