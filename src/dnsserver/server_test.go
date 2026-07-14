// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dnsserver

import (
	"net"
	"net/netip"
	"testing"

	"github.com/google/uuid"
	"github.com/miekg/dns"

	"github.com/podomy/concord/src/peerdiscovery"
)

func TestPopulateSRV(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	nodes := []peerdiscovery.Node{
		{
			ID:      id,
			Address: netip.MustParseAddrPort("10.0.0.5:7946"),
			State:   peerdiscovery.NodeStateAlive,
		},
	}

	msg := &dns.Msg{}
	name := peerdiscovery.DNSService + "."
	(&handler{}).populateSRV(msg, nodes, name)

	if msg.Rcode != dns.RcodeSuccess && msg.Rcode != 0 {
		t.Fatalf("rcode = %d", msg.Rcode)
	}
	if len(msg.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(msg.Answer))
	}
	srv, ok := msg.Answer[0].(*dns.SRV)
	if !ok {
		t.Fatalf("answer type %T", msg.Answer[0])
	}
	wantTarget := id.String() + tldConcord
	if srv.Target != wantTarget {
		t.Fatalf("target = %q, want %q", srv.Target, wantTarget)
	}
	if srv.Port != 7946 {
		t.Fatalf("port = %d, want 7946", srv.Port)
	}
	if len(msg.Extra) != 1 {
		t.Fatalf("extra = %d, want 1", len(msg.Extra))
	}
	a, ok := msg.Extra[0].(*dns.A)
	if !ok {
		t.Fatalf("extra type %T", msg.Extra[0])
	}
	if !a.A.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("A = %v, want 10.0.0.5", a.A)
	}
}

func TestPopulateSRVNonConcordName(t *testing.T) {
	t.Parallel()

	msg := &dns.Msg{}
	(&handler{}).populateSRV(msg, nil, "something.else.")
	if msg.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want NXDOMAIN", msg.Rcode)
	}
}

func TestPopulateA(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	other := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	nodes := []peerdiscovery.Node{
		{ID: other, Address: netip.MustParseAddrPort("10.0.0.9:7946")},
		{ID: id, Address: netip.MustParseAddrPort("10.0.0.5:7946")},
	}

	msg := &dns.Msg{}
	name := id.String() + tldConcord
	(&handler{}).populateA(msg, nodes, name)

	if len(msg.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(msg.Answer))
	}
	a, ok := msg.Answer[0].(*dns.A)
	if !ok {
		t.Fatalf("answer type %T", msg.Answer[0])
	}
	if !a.A.Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("A = %v, want 10.0.0.5", a.A)
	}
}

func TestPopulateANoMatch(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	nodes := []peerdiscovery.Node{
		{ID: id, Address: netip.MustParseAddrPort("10.0.0.5:7946")},
	}

	msg := &dns.Msg{}
	(&handler{}).populateA(msg, nodes, "unknown.concord.local.")
	if len(msg.Answer) != 0 {
		t.Fatalf("answers = %d, want 0", len(msg.Answer))
	}
}
