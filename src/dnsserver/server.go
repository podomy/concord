// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dnsserver implements an embedded DNS server that serves Concord's
// peer membership via SRV and A records. Other Concord nodes query this
// server to discover the full memberlist, enabling cross-subnet discovery.
package dnsserver

import (
	"context"
	"fmt"
	"net"
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
		h.populateSRV(&msg, nodes, r.Question[0].Name)
	case dns.TypeA:
		h.populateA(&msg, nodes, r.Question[0].Name)
	}

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

// populateSRV fills msg with SRV records for all known members and
// their corresponding A records in the additional section.
func (h *handler) populateSRV(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
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

// populateA fills msg with A records for members matching the queried hostname.
func (h *handler) populateA(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
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

// Start launches an embedded DNS server that serves Concord peer membership.
// Empty addr binds to ":"+DNSPort. Bind is checked before return so a second
// instance fails fast if the port is taken. The server runs until ctx is done.
func Start(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger, addr string) error {
	if addr == "" {
		addr = ":" + peerdiscovery.DNSPort
	}

	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, "udp", addr)
	if err != nil {
		return fmt.Errorf("dns listen %s: %w", addr, err)
	}

	srv := &dns.Server{
		PacketConn: pc,
		Handler:    &handler{memberService: memberService},
	}

	// Graceful shutdown.
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(); err != nil {
			logger.Error("dns server shutdown", zap.Error(err))
		}
	}()

	go func() {
		if err := srv.ActivateAndServe(); err != nil {
			logger.Error("dns server", zap.Error(err))
		}
	}()

	return nil
}
