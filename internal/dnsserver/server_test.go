// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dnsserver

import (
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/miekg/dns"

	"github.com/podomy/concord/internal/peerdiscovery"
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
	if msg.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want NXDOMAIN", msg.Rcode)
	}
	if len(msg.Answer) != 0 {
		t.Fatalf("answers = %d, want 0", len(msg.Answer))
	}
}

// A query for a name outside our suffix is also NXDOMAIN: this server is
// authoritative only for our zone.
func TestPopulateAOutsideSuffix(t *testing.T) {
	t.Parallel()

	msg := &dns.Msg{}
	(&handler{}).populateA(msg, nil, "example.com.")
	if msg.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want NXDOMAIN", msg.Rcode)
	}
}

// AAAA answers serve IPv6 members; A answers never carry IPv6 addresses.
func TestPopulateAAAA(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	v4 := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	nodes := []peerdiscovery.Node{
		{ID: id, Address: netip.MustParseAddrPort("[2001:db8::5]:7946")},
		{ID: v4, Address: netip.MustParseAddrPort("10.0.0.5:7946")},
	}

	msg := &dns.Msg{}
	name := id.String() + tldConcord
	(&handler{}).populateAAAA(msg, nodes, name)
	if len(msg.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(msg.Answer))
	}
	aaaa, ok := msg.Answer[0].(*dns.AAAA)
	if !ok {
		t.Fatalf("answer type %T", msg.Answer[0])
	}
	if !aaaa.AAAA.Equal(net.ParseIP("2001:db8::5")) {
		t.Fatalf("AAAA = %v, want 2001:db8::5", aaaa.AAAA)
	}

	v4msg := &dns.Msg{}
	(&handler{}).populateAAAA(v4msg, nodes, v4.String()+tldConcord)
	if len(v4msg.Answer) != 0 {
		t.Fatalf("answers = %d, want 0 (NODATA for v4-only member)", len(v4msg.Answer))
	}
	if v4msg.Rcode == dns.RcodeNameError {
		t.Fatal("known name must not return NXDOMAIN")
	}
}

// AAAA for an unknown name is NXDOMAIN, matching A and SRV.
func TestPopulateAAAANoMatch(t *testing.T) {
	t.Parallel()

	msg := &dns.Msg{}
	(&handler{}).populateAAAA(msg, nil, "unknown.concord.local.")
	if msg.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want NXDOMAIN", msg.Rcode)
	}
}

// Name matching is case-insensitive after Fqdn normalization.
func TestPopulateACaseInsensitive(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	nodes := []peerdiscovery.Node{
		{ID: id, Address: netip.MustParseAddrPort("10.0.0.5:7946")},
	}

	msg := &dns.Msg{}
	(&handler{}).populateA(msg, nodes, strings.ToUpper(id.String())+".CONCORD.LOCAL")
	if len(msg.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(msg.Answer))
	}
}

// SRV extras use AAAA for IPv6 members instead of a malformed A record.
func TestPopulateSRVIPv6Extra(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	nodes := []peerdiscovery.Node{
		{ID: id, Address: netip.MustParseAddrPort("[2001:db8::5]:7946")},
	}

	msg := &dns.Msg{}
	(&handler{}).populateSRV(msg, nodes, peerdiscovery.DNSService+".")
	if len(msg.Extra) != 1 {
		t.Fatalf("extra = %d, want 1", len(msg.Extra))
	}
	aaaa, ok := msg.Extra[0].(*dns.AAAA)
	if !ok {
		t.Fatalf("extra type %T", msg.Extra[0])
	}
	if !aaaa.AAAA.Equal(net.ParseIP("2001:db8::5")) {
		t.Fatalf("AAAA = %v, want 2001:db8::5", aaaa.AAAA)
	}
}

// Oversize answers truncate with TC set instead of fragmenting: no TCP
// fallback exists.
func TestTruncateToFit(t *testing.T) {
	t.Parallel()

	msg := &dns.Msg{}
	for range 40 {
		msg.Answer = append(msg.Answer, &dns.SRV{
			Hdr:    dns.RR_Header{Name: "x.", Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
			Target: "f7daefa1-a223-4480-a1ca-3b8b07e7472e.concord.local.",
			Port:   7946,
		})
	}
	truncateToFit(msg)
	if !msg.Truncated {
		t.Fatal("expected TC bit set")
	}
	if msg.Len() > maxUDPResponse {
		t.Fatalf("len = %d, want <= %d", msg.Len(), maxUDPResponse)
	}
}

// A fitting message never sets TC.
func TestTruncateToFitKeepsSmall(t *testing.T) {
	t.Parallel()

	msg := &dns.Msg{}
	msg.Answer = append(msg.Answer, &dns.SRV{
		Hdr:    dns.RR_Header{Name: "x.", Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
		Target: "f7daefa1-a223-4480-a1ca-3b8b07e7472e.concord.local.",
		Port:   7946,
	})
	truncateToFit(msg)
	if msg.Truncated {
		t.Fatal("TC set on a fitting message")
	}
}

// Nil member service fails closed with SERVFAIL instead of panicking.
func TestServeDNSNilService(t *testing.T) {
	t.Parallel()

	req := &dns.Msg{}
	req.SetQuestion("f7daefa1-a223-4480-a1ca-3b8b07e7472e.concord.local.", dns.TypeA)
	w := &stubResponseWriter{}
	(&handler{}).ServeDNS(w, req)
	if w.msg == nil {
		t.Fatal("no reply written")
	}
	if w.msg.Rcode != dns.RcodeServerFailure {
		t.Fatalf("rcode = %d, want SERVFAIL", w.msg.Rcode)
	}
}

// Question counts other than one fail with FORMERR.
func TestServeDNSQuestionCount(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 2} {
		req := &dns.Msg{}
		req.Question = make([]dns.Question, n)
		for i := range req.Question {
			req.Question[i] = dns.Question{
				Name:   "x.concord.local.",
				Qtype:  dns.TypeA,
				Qclass: dns.ClassINET,
			}
		}
		w := &stubResponseWriter{}
		(&handler{memberService: nil}).ServeDNS(w, req)
		if w.msg == nil {
			t.Fatalf("questions=%d: no reply written", n)
		}
		if w.msg.Rcode != dns.RcodeFormatError {
			t.Fatalf("questions=%d: rcode = %d, want FORMERR", n, w.msg.Rcode)
		}
	}
}

// stubResponseWriter captures the reply for ServeDNS unit tests.
type stubResponseWriter struct {
	msg *dns.Msg
}

func (s *stubResponseWriter) LocalAddr() net.Addr  { return nil }
func (s *stubResponseWriter) RemoteAddr() net.Addr { return nil }
func (s *stubResponseWriter) WriteMsg(m *dns.Msg) error {
	s.msg = m
	return nil
}
func (s *stubResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (s *stubResponseWriter) Close() error                { return nil }
func (s *stubResponseWriter) TsigStatus() error           { return nil }
func (s *stubResponseWriter) TsigTimersOnly(bool)         {}
func (s *stubResponseWriter) Hijack()                     {}
