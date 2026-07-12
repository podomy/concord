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
)

type MDNSResolver struct {
	Timeout time.Duration
}

// Resolve discovers Concord peers on the local network using multicast DNS.
// It browses for the configured mDNS service and returns discovered peer addresses.
func (m *MDNSResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	// Browse the local network for the concord nodes
	serviceEntries, err := m.mdnsBrowse(ctx)
	if err != nil {
		return nil, fmt.Errorf("browsing the local network: %w", err)
	}

	var addresses []netip.AddrPort
	for _, entry := range serviceEntries {
		addr, ok := entryToAddrPort(entry)
		if !ok {
			continue
		}
		addresses = append(addresses, addr)
	}

	return addresses, nil
}

// entryToAddrPort extracts a netip.AddrPort from an mDNS service entry.
// It returns false if the entry has no valid address or port.
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
	var wg sync.WaitGroup

	wg.Go(func() {
		for entry := range entriesCh {
			entries = append(entries, entry)
		}
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

	return entries, nil
}
