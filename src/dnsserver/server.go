// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dnsserver

import (
	"fmt"
	"net"

	"github.com/miekg/dns"
)

type handler struct{}

func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	msg := dns.Msg{}
	msg.SetReply(r)
	switch r.Question[0].Qtype {
	// return SRV record pointing to this node
	case dns.TypeSRV:
		msg.Answer = append(msg.Answer, &dns.SRV{
			Hdr:    dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 60},
			Target: "node1.concord.local.",
			Port:   7946,
		})
	// return A record for target
	case dns.TypeA:
		msg.Answer = append(msg.Answer, &dns.A{
			Hdr: dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
			A:   net.ParseIP("10.0.0.1"),
		})
	}

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

func Start() error {
	srv := &dns.Server{Addr: ":8053", Net: "udp", Handler: &handler{}}
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("dns server: %w", err)
	}
	return nil
}
