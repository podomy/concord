// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/miekg/dns"
)

func TestExtractAddrFromExtra(t *testing.T) {
	t.Parallel()

	target := "f7daefa1-a223-4480-a1ca-3b8b07e7472e.concord.local."
	other := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.concord.local."
	port := uint16(7946)

	extra := []dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{Name: other, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("10.0.0.99").To4(),
		},
		&dns.A{
			Hdr: dns.RR_Header{Name: target, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("10.0.0.5").To4(),
		},
		&dns.SRV{
			Hdr: dns.RR_Header{Name: DNSService, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
		},
	}

	got := extractAddrFromExtra(extra, target, port)
	if len(got) != 1 {
		t.Fatalf("expected 1 address, got %d: %v", len(got), got)
	}

	want := netip.MustParseAddrPort("10.0.0.5:7946")
	if got[0] != want {
		t.Fatalf("got %v, want %v", got[0], want)
	}
}

func TestExtractAddrFromExtraNoMatch(t *testing.T) {
	t.Parallel()

	extra := []dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{Name: "other.concord.local.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("10.0.0.5").To4(),
		},
	}
	got := extractAddrFromExtra(extra, "missing.concord.local.", 7946)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestDNSSRVResolverCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &DNSSRVResolver{Bootstrap: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")}}
	_, err := r.Resolve(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestDNSSRVResolverAllQueriesFail(t *testing.T) {
	t.Parallel()

	// Unreachable bootstrap peer; DNS query to 127.0.0.1:8053 should fail quickly.
	r := &DNSSRVResolver{
		Bootstrap: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")},
		Timeout:   1,
	}
	_, err := r.Resolve(context.Background())
	if err == nil {
		t.Fatal("expected error when all DNS SRV queries fail")
	}
}
