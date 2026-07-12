// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dnsserver implements an embedded DNS server that serves Concord's
// peer membership via SRV and A records. Other Concord nodes query this
// server to discover the full memberlist, enabling cross-subnet discovery.
package dnsserver

import (
	"context"
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
	case dns.TypeSRV:
		h.serveSRV(&msg, nodes, r.Question[0].Name)
	case dns.TypeA:
		h.serveA(&msg, nodes, r.Question[0].Name)
	}

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

// serveSRV populates msg with SRV records for all known members and
// their corresponding A records in the additional section.
func (h *handler) serveSRV(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
	if !strings.HasPrefix(name, peerdiscovery.DNSService) {
		msg.Rcode = dns.RcodeNameError
		return
	}

	for _, node := range nodes {
		msg.Answer = append(msg.Answer, &dns.SRV{
			Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
			Target: node.ID.String() + tldConcord,
			Port:   node.Address.Port(),
		})
		msg.Extra = append(msg.Extra, &dns.A{
			Hdr: dns.RR_Header{Name: dns.Fqdn(node.ID.String() + tldConcord), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   node.Address.Addr().AsSlice(),
		})
	}
}

// serveA populates msg with A records for members matching the queried hostname.
func (h *handler) serveA(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
	for _, node := range nodes {
		target := node.ID.String() + tldConcord
		if dns.Fqdn(name) == target {
			msg.Answer = append(msg.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
				A:   node.Address.Addr().AsSlice(),
			})
		}
	}
}

// Start launches an embedded DNS server on port 8053 that serves Concord's
// peer membership from the provided MemberService. It runs in a background
// goroutine and returns immediately. Errors from ListenAndServe are logged.
func Start(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger) error {
	srv := &dns.Server{Addr: ":" + peerdiscovery.DNSPort, Net: "udp", Handler: &handler{memberService: memberService}}

	// Graceful shutdown.
	go func() {
		<-ctx.Done()

		if err := srv.Shutdown(); err != nil {
			logger.Error("dns server shutdown", zap.Error(err))
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			logger.Error("dns server", zap.Error(err))
		}
	}()

	return nil
}
