// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dnsserver

import (
	"fmt"

	"github.com/miekg/dns"

	"github.com/podomy/concord/src/peerdiscovery"
)

type handler struct {
	memberService *peerdiscovery.MemberService
}

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
	case dns.TypeSRV:
		for _, node := range nodes {
			msg.Answer = append(msg.Answer, &dns.SRV{
				Hdr:    dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
				Target: node.ID.String() + ".concord.local.",
				Port:   node.Address.Port(),
			})
		}
	// return all member we know as A records
	case dns.TypeA:
		for _, node := range nodes {
			msg.Answer = append(msg.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
				A:   node.Address.Addr().AsSlice(),
			})
		}
	}

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

func Start(memberService *peerdiscovery.MemberService) error {
	srv := &dns.Server{Addr: ":8053", Net: "udp", Handler: &handler{memberService: memberService}}
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("dns server: %w", err)
	}
	return nil
}
