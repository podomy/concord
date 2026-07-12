// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dnsserver implements an embedded DNS server that serves Concord's
// peer membership via SRV and A records. Other Concord nodes query this
// server to discover the full memberlist, enabling cross-subnet discovery.
package dnsserver

import (
	"strings"

	"github.com/miekg/dns"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/peerdiscovery"
)

// tldConcord is a fake TLD used inside the DNS server to form per-node
// hostnames. Each member gets a name like <UUID>.concord.local. These names
// are never resolved on the public internet.
const tldConcord = ".concord.local."

// handler implements dns.Handler to serve Concord peer discovery records.
type handler struct {
	memberService *peerdiscovery.MemberService
}

// ServeDNS responds to DNS queries for Concord's peer mesh. It handles two
// query types:
//
//   - SRV: returns all known members with targets of the form
//     <UUID>.concord.local. A records for each target are included in the
//     response's additional section so clients can resolve addresses in a
//     single round trip.
//   - A: returns the IP address for a specific <UUID>.concord.local. target.
//
// Non-Concord SRV queries receive NXDOMAIN. Other query types return an empty
// response.
func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	msg := dns.Msg{}
	msg.SetReply(r)

	nodes, err := h.memberService.Members()
	if err != nil {
		msg.Rcode = dns.RcodeServerFailure
		_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
		return
	}

	switch r.Question[0].Qtype {
	// return all members we know as SRV records
	// SRV query: _concord._tcp.<domain>
	case dns.TypeSRV:
		if !strings.HasPrefix(r.Question[0].Name, peerdiscovery.DNSService) {
			msg.Rcode = dns.RcodeNameError
			_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
			return
		}

		for _, node := range nodes {
			msg.Answer = append(msg.Answer, &dns.SRV{
				Hdr:    dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
				Target: node.ID.String() + tldConcord,
				Port:   node.Address.Port(),
			})
			msg.Extra = append(msg.Extra, &dns.A{
				Hdr: dns.RR_Header{Name: dns.Fqdn(node.ID.String() + tldConcord), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
				A:   node.Address.Addr().AsSlice(),
			})
		}
	// return all member we know as A records
	// A query: <UUID>.concord.local.
	case dns.TypeA:
		for _, node := range nodes {
			target := node.ID.String() + tldConcord
			if dns.Fqdn(r.Question[0].Name) == target {
				msg.Answer = append(msg.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
					A:   node.Address.Addr().AsSlice(),
				})
			}
		}
	}

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

// Start launches an embedded DNS server on port 8053 that serves Concord's
// peer membership from the provided MemberService. It runs in a background
// goroutine and returns immediately. Errors from ListenAndServe are logged.
func Start(memberService *peerdiscovery.MemberService, logger *zap.Logger) error {
	srv := &dns.Server{Addr: ":" + peerdiscovery.DNSPort, Net: "udp", Handler: &handler{memberService: memberService}}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Error("dns server", zap.Error(err))
		}
	}()

	return nil
}
