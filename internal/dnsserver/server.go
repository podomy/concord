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
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/peerdiscovery"
)

// tldConcord is a fake TLD used inside the DNS server to form per-node
// hostnames. Each member gets a name like <UUID>.concord.local. These names
// are never resolved on the public internet.
const tldConcord = ".concord.local."

// maxUDPResponse caps DNS/UDP answers at the classic 512-byte ceiling. The
// server has no TCP listener and answers no EDNS, so anything past the cap
// truncates with the TC bit set (see truncateToFit) instead of fragmenting.
// Clients must treat TC as incomplete; there is no TCP fallback.
const maxUDPResponse = 512

// dnsTTL is the TTL in seconds on every SRV, A, and AAAA answer. Ten
// seconds tracks the 5s membership churn: a dead member lingers at most two
// beats in caches instead of a minute. The tradeoff is more queries.
const dnsTTL = 10

// handler implements dns.Handler to serve Concord peer discovery records.
type handler struct {
	memberService *peerdiscovery.MemberService

	mu       sync.Mutex
	cached   []peerdiscovery.Node
	cachedAt time.Time
}

// membersCacheTTL bounds how long one memberlist snapshot serves DNS
// queries. Membership churns on a 5s beat while floods arrive in
// milliseconds; without this each UDP query would take the memberlist
// lock. Staleness is capped at one second, far below the 10s record
// TTL clients already cache.
const membersCacheTTL = time.Second

// members returns the cached snapshot when fresh, refreshing under the
// mutex otherwise. Only successful snapshots cache; errors always
// reach the caller so a transient failure never pins stale data.
func (h *handler) members() ([]peerdiscovery.Node, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cached != nil && time.Since(h.cachedAt) < membersCacheTTL {
		return h.cached, nil
	}
	nodes, err := h.memberService.Members()
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	h.cached = nodes
	h.cachedAt = time.Now()
	return nodes, nil
}

// ServeDNS responds to DNS queries for Concord's peer mesh. It handles three
// query types:
//
//   - SRV: returns all known members with targets of the form
//     <UUID>.concord.local. A or AAAA records for each target are included
//     in the response's additional section so clients can resolve addresses
//     in a single round trip.
//   - A: returns the IPv4 address for a specific <UUID>.concord.local.
//     target.
//   - AAAA: returns the IPv6 address for a specific <UUID>.concord.local.
//     target.
//
// Unknown names return NXDOMAIN for every type this server answers: a name
// under our suffix with no member, and any other name, are both outside the
// served zone. A known name with no record of the asked type returns empty
// NOERROR (NODATA), per DNS convention. Other query types return an empty
// response.
//
// Names compare case-insensitively after Fqdn normalization. Oversized
// answers truncate with TC set; see maxUDPResponse.
func (h *handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	msg := dns.Msg{}
	msg.SetReply(r)

	if len(r.Question) != 1 {
		// Construct the error response.
		msg.SetRcodeFormatError(r)

		_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
		return
	}

	if h == nil || h.memberService == nil {
		msg.Rcode = dns.RcodeServerFailure
		_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
		return
	}

	nodes, err := h.members()
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
	case dns.TypeAAAA:
		h.populateAAAA(&msg, nodes, r.Question[0].Name)
	}

	truncateToFit(&msg)

	_ = w.WriteMsg(&msg) //nolint:errcheck // best-effort write
}

// populateSRV fills msg with SRV records for alive members only and
// their corresponding address records in the additional section: A for
// IPv4 members, AAAA for IPv6 members. A member address that is neither
// (should not happen) contributes no additional record.
//
// Only alive members serve: suspect may already be partitioned, and
// dead, left, or unknown members cannot answer. A newcomer learns them
// on the next round after they go alive. By-name A and AAAA lookups
// below stay state-independent: an explicit name query answers from the
// last known address, while SRV (the discovery path) never points at a
// member that cannot answer.
func (h *handler) populateSRV(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
	service := dns.Fqdn(peerdiscovery.DNSService)
	if !strings.HasPrefix(strings.ToLower(dns.Fqdn(name)), strings.ToLower(service)) {
		msg.Rcode = dns.RcodeNameError
		return
	}

	for _, node := range nodes {
		if node.State != peerdiscovery.NodeStateAlive {
			continue
		}
		msg.Answer = append(msg.Answer, &dns.SRV{
			Hdr:    dns.RR_Header{Name: name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: dnsTTL},
			Target: node.ID.String() + tldConcord,
			Port:   node.Address.Port(),
		})
		addr := node.Address.Addr().Unmap()
		if addr.IsValid() && (addr.Is4() || addr.Is6()) {
			msg.Extra = append(
				msg.Extra,
				addrRecord(dns.Fqdn(node.ID.String()+tldConcord), addr, addr.Is6()),
			)
		}
	}
}

// populateA fills msg with A records for members matching the queried
// hostname. Comparison is case-insensitive after Fqdn normalization. A
// known name on an IPv6-only member returns empty NOERROR (NODATA); an
// unknown name returns NXDOMAIN, matching populateSRV.
func (h *handler) populateA(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
	h.populateAddr(msg, nodes, name, false)
}

// populateAAAA fills msg with AAAA records for members matching the queried
// hostname. Comparison is case-insensitive after Fqdn normalization. A
// known name on an IPv4-only member returns empty NOERROR (NODATA); an
// unknown name returns NXDOMAIN, matching populateSRV.
func (h *handler) populateAAAA(msg *dns.Msg, nodes []peerdiscovery.Node, name string) {
	h.populateAddr(msg, nodes, name, true)
}

// populateAddr fills msg with A (wantV6 false) or AAAA (wantV6 true) records
// for the member matching name. A known name without a record of the asked
// type returns empty NOERROR (NODATA); an unknown name returns NXDOMAIN.
//
// A position of (0, 0) is unknown, not a filter: it is Valid per geo, so a
// member with no fix still gets records here. By-name answers stay
// state-independent; only SRV filters on liveness.
func (h *handler) populateAddr(msg *dns.Msg, nodes []peerdiscovery.Node, name string, wantV6 bool) {
	for _, node := range nodes {
		target := node.ID.String() + tldConcord
		if !strings.EqualFold(dns.Fqdn(name), target) {
			continue
		}
		if addr := node.Address.Addr().Unmap(); addr.IsValid() && addr.Is6() == wantV6 {
			msg.Answer = append(msg.Answer, addrRecord(name, addr, wantV6))
		}
		return
	}
	msg.Rcode = dns.RcodeNameError
}

// addrRecord builds the A or AAAA answer for a normalized member address.
// Owner name, class, and TTL match the SRV answers; only the type follows
// the address family.
func addrRecord(name string, addr netip.Addr, wantV6 bool) dns.RR {
	if wantV6 {
		return &dns.AAAA{
			Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: dnsTTL},
			AAAA: addr.AsSlice(),
		}
	}
	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: dnsTTL},
		A:   addr.AsSlice(),
	}
}

// truncateToFit drops additional then answer records until msg fits
// maxUDPResponse, setting the TC bit when anything was dropped. Clients
// must treat TC as incomplete; there is no TCP fallback and no EDNS.
func truncateToFit(msg *dns.Msg) {
	for msg.Len() > maxUDPResponse && (len(msg.Answer) > 0 || len(msg.Extra) > 0) {
		if len(msg.Extra) > 0 {
			msg.Extra = msg.Extra[:len(msg.Extra)-1]
		} else {
			msg.Answer = msg.Answer[:len(msg.Answer)-1]
		}
		msg.Truncated = true
	}
}

// dnsMu guards dnsSrv: Start registers the server, Stop and the ctx
// goroutine each take it at most once, so shutdown runs exactly once.
var (
	dnsMu  sync.Mutex
	dnsSrv *dns.Server
)

// Stop shuts down the server started by Start. It is idempotent and safe
// when Start never ran: with no server registered it is a no-op, and a
// second call after shutdown (or after context cancel) shuts down
// nothing. Runtime teardown ordering relies on this: Stop runs even on
// paths where Start failed or the context already fired.
func Stop() {
	srv := takeDNSServer()
	if srv == nil {
		return
	}
	_ = srv.Shutdown() //nolint:errcheck // second shutdown after ctx cancel only reports already-stopped; nothing to do
}

// takeDNSServer removes and returns the registered server, or nil when
// none is registered.
func takeDNSServer() *dns.Server {
	dnsMu.Lock()
	defer dnsMu.Unlock()

	srv := dnsSrv
	dnsSrv = nil
	return srv
}

// Start launches an embedded DNS server that serves Concord peer membership.
// Empty addr binds to ":"+DNSPort. Bind is checked before return so a second
// instance fails fast if the port is taken. The server runs until ctx is done.
//
// Serves UDP only: no TCP listener and no EDNS. Oversized answers truncate
// with TC set; see maxUDPResponse.
func Start(ctx context.Context, memberService *peerdiscovery.MemberService, logger *logs.Logger, addr string) error {
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

	dnsMu.Lock()
	dnsSrv = srv
	dnsMu.Unlock()

	// Graceful shutdown: take the handle so Stop and cancel cannot both
	// shut down, which would log a spurious already-stopped error.
	go func() {
		<-ctx.Done()
		doneSrv := takeDNSServer()
		if doneSrv == nil {
			return
		}
		if err := doneSrv.Shutdown(); err != nil {
			logger.Error("dns server shutdown", logs.Error(err))
		}
	}()

	go func() {
		if err := srv.ActivateAndServe(); err != nil {
			logger.Error("dns server", logs.Error(err))
		}
	}()

	return nil
}
