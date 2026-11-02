// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dnsserver

import (
	"net/netip"
	"testing"

	"github.com/google/uuid"
	"github.com/miekg/dns"

	"github.com/podomy/concord/internal/peerdiscovery"
)

// Every answer carries the short TTL that tracks the 5s membership churn:
// SRV answers and their additional records, plus by-name A and AAAA.
func TestAnswersCarryShortTTL(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("f7daefa1-a223-4480-a1ca-3b8b07e7472e")
	v6 := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	nodes := []peerdiscovery.Node{
		{ID: id, Address: netip.MustParseAddrPort("10.0.0.5:7946"), State: peerdiscovery.NodeStateAlive},
		{ID: v6, Address: netip.MustParseAddrPort("[2001:db8::5]:7946"), State: peerdiscovery.NodeStateAlive},
	}

	srvMsg := &dns.Msg{}
	(&handler{}).populateSRV(srvMsg, nodes, peerdiscovery.DNSService+".")
	if len(srvMsg.Answer) != 2 {
		t.Fatalf("srv answers = %d, want 2", len(srvMsg.Answer))
	}
	for _, rr := range append(append([]dns.RR{}, srvMsg.Answer...), srvMsg.Extra...) {
		if rr.Header().Ttl != dnsTTL {
			t.Fatalf("%v ttl = %d, want %d", rr, rr.Header().Ttl, dnsTTL)
		}
	}

	aMsg := &dns.Msg{}
	(&handler{}).populateA(aMsg, nodes, id.String()+tldConcord)
	if len(aMsg.Answer) != 1 || aMsg.Answer[0].Header().Ttl != dnsTTL {
		t.Fatalf("a answer = %v, want one record with ttl %d", aMsg.Answer, dnsTTL)
	}

	aaaaMsg := &dns.Msg{}
	(&handler{}).populateAAAA(aaaaMsg, nodes, v6.String()+tldConcord)
	if len(aaaaMsg.Answer) != 1 || aaaaMsg.Answer[0].Header().Ttl != dnsTTL {
		t.Fatalf("aaaa answer = %v, want one record with ttl %d", aaaaMsg.Answer, dnsTTL)
	}
}
