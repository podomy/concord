// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"net/netip"

	"github.com/google/uuid"
)

// NodeState describes memberlist's current liveness observation for a node.
type NodeState string

const (
	NodeStateAlive   NodeState = "alive"
	NodeStateSuspect NodeState = "suspect"
	NodeStateDead    NodeState = "dead"
	NodeStateLeft    NodeState = "left"
	NodeStateUnknown NodeState = "unknown"
)

// Node identifies a Concord node as it appears in peer discovery.
//
// ID is the stable Concord node identity. Address is the current network
// endpoint used by memberlist for peer membership traffic. The address can
// change over time; the ID is the durable identity. State is memberlist's
// current liveness observation for the node.
type Node struct {
	ID      uuid.UUID
	Address netip.AddrPort
	State   NodeState
}

// Resolver discovers candidate peer addresses for bootstrapping memberlist
// membership. Different implementations cover different discovery mechanisms
// such as LAN broadcast, DNS SRV records, or a rendezvous point.
//
// Resolve must be safe for concurrent calls from multiple goroutines.
type Resolver interface {
	Resolve(ctx context.Context) ([]netip.AddrPort, error)
}

// ResolverFunc is an adapter that turns a plain function into a Resolver.
type ResolverFunc func(ctx context.Context) ([]netip.AddrPort, error)

// Resolve calls the underlying function.
func (f ResolverFunc) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	return f(ctx)
}

// MultiResolver merges candidates from multiple resolvers. Each resolver is
// called and results are deduplicated by their string representation.
type MultiResolver struct {
	resolvers []Resolver
}

// NewMultiResolver returns a resolver that queries all provided resolvers.
func NewMultiResolver(resolvers ...Resolver) *MultiResolver {
	return &MultiResolver{resolvers: resolvers}
}

// Resolve calls every configured resolver and deduplicates the results.
func (m *MultiResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	type result struct {
		addrs []netip.AddrPort
		err   error
	}

	ch := make(chan result, len(m.resolvers))
	for _, r := range m.resolvers {
		go func() {
			addrs, err := r.Resolve(ctx)
			ch <- result{addrs: addrs, err: err}
		}()
	}

	seen := map[string]struct{}{}
	var out []netip.AddrPort

	for range m.resolvers {
		res := <-ch
		if res.err != nil {
			continue
		}

		for _, addr := range res.addrs {
			s := addr.String()
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, addr)
		}
	}

	return out, nil
}

// Default mDNS and DNS SRV service identifiers for Concord peer discovery.
// Implementations should use these constants to ensure interoperability.
const (
	MDNSService   = "_concord._udp"
	DNSSRVService = "_concord._tcp"
)
