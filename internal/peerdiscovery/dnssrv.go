// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/miekg/dns"
)

// DNSSRVResolver discovers peer addresses by querying known Concord nodes'
// embedded DNS servers for SRV records. It uses the bootstrap addresses
// (typically discovered via mDNS) as seeds and collects the full memberlist
// from each one.
type DNSSRVResolver struct {
	// Bootstrap are candidate peer addresses (host:memberlist-port) whose
	// embedded DNS servers will be queried for SRV records.
	Bootstrap []netip.AddrPort
	// Timeout for each DNS query made to a bootstrap peer.
	Timeout time.Duration
	// QueryPort is the UDP port of the peer's embedded DNS server.
	// Empty means DNSPort (8053).
	QueryPort string
}

// Resolve queries every bootstrap peer's DNS server for SRV records and
// returns the deduplicated set of discovered peer addresses. Resolve returns
// an error only if all bootstrap queries fail.
//
// When Bootstrap is empty, Resolve runs an mDNS query first to discover
// candidate peers on the local network and uses those as bootstrap seeds.
func (d *DNSSRVResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	var results []netip.AddrPort
	var errs []error

	bootstrap := d.Bootstrap
	if len(bootstrap) == 0 {
		mdnsResolver := MDNSResolver{}
		addrs, err := mdnsResolver.Resolve(ctx)
		if err != nil {
			return nil, fmt.Errorf("internal mDNS seed: %w", err)
		}
		bootstrap = addrs
	}

	for _, peer := range bootstrap {
		addrs, err := d.queryPeer(ctx, peer)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		results = append(results, addrs...)
	}

	if len(results) == 0 && len(errs) > 0 {
		return nil, fmt.Errorf("all dns srv queries failed: %w", errors.Join(errs...))
	}

	return results, nil
}

// queryPeer sends an SRV query to a single peer's embedded DNS server and
// extracts the discovered addresses from the response's additional section.
func (d *DNSSRVResolver) queryPeer(ctx context.Context, peer netip.AddrPort) ([]netip.AddrPort, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(DNSService), dns.TypeSRV)
	m.RecursionDesired = false

	queryTimeout := d.Timeout
	if queryTimeout == 0 {
		queryTimeout = 5 * time.Second
	}
	dnsPort := d.QueryPort
	if dnsPort == "" {
		dnsPort = DNSPort
	}
	c := &dns.Client{Timeout: queryTimeout}
	resp, _, err := c.Exchange(m, net.JoinHostPort(peer.Addr().String(), dnsPort))
	if err != nil {
		return nil, fmt.Errorf("srv query to %s: %w", peer, err)
	}
	if resp.Rcode != dns.RcodeSuccess {
		return nil, fmt.Errorf("srv query to %s: rcode %d", peer, resp.Rcode)
	}

	var results []netip.AddrPort
	for _, ans := range resp.Answer {
		srv, ok := ans.(*dns.SRV)
		if !ok {
			continue
		}

		addrs := extractAddrFromExtra(resp.Extra, srv.Target, srv.Port)
		results = append(results, addrs...)
	}

	return results, nil
}

// extractAddrFromExtra scans the additional section for A records whose name
// matches the given target and pairs them with the provided port.
func extractAddrFromExtra(extra []dns.RR, target string, port uint16) []netip.AddrPort {
	var results []netip.AddrPort
	for _, rr := range extra {
		arecord, ok := rr.(*dns.A)
		if !ok || arecord.Hdr.Name != target {
			continue
		}

		addr, ok := netip.AddrFromSlice(arecord.A)
		if !ok {
			continue
		}

		results = append(results, netip.AddrPortFrom(addr.Unmap(), port))
	}
	return results
}
