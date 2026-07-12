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
	// Timeout for each DNS query made to a bootstrap peer.
	Timeout time.Duration
	// Bootstrap are candidate peer addresses (host:memberlist-port) whose
	// embedded DNS servers will be queried for SRV records.
	Bootstrap []netip.AddrPort
}

// Resolve queries every bootstrap peer's DNS server for SRV records and
// returns the deduplicated set of discovered peer addresses. Resolve returns
// an error only if all bootstrap queries fail.
func (d *DNSSRVResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	var results []netip.AddrPort
	var errs []error
	for _, peer := range d.Bootstrap {
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
	m.SetQuestion(DNSService, dns.TypeSRV)
	m.RecursionDesired = false

	c := &dns.Client{Timeout: d.Timeout}
	resp, _, err := c.Exchange(m, net.JoinHostPort(peer.Addr().String(), DNSPort))
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

		for _, extra := range resp.Extra {
			arecord, ok := extra.(*dns.A)
			if !ok || arecord.Hdr.Name != srv.Target {
				continue
			}

			addr, ok := netip.AddrFromSlice(arecord.A)
			if !ok {
				continue
			}
			addrPort := netip.AddrPortFrom(addr.Unmap(), srv.Port)

			results = append(results, addrPort)
		}
	}

	return results, nil
}
