// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"errors"
	"fmt"
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
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	// All of the addresses we will get from the
	// resolvers will be stored here.
	addresses := make(map[netip.AddrPort]struct{}, 0)

	// We fail only if all of the resolvers fail
	var errs []error
	for _, resolver := range m.resolvers {
		tempAddresses, err := resolver.Resolve(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		for _, address := range tempAddresses {
			addresses[address] = struct{}{}
		}

	}

	if len(addresses) == 0 && len(errs) > 0 {
		combinedError := errors.Join(errs...)
		return nil, fmt.Errorf("all resolvers failed: %w", combinedError)
	}

	result := make([]netip.AddrPort, 0, len(addresses))
	for address := range addresses {
		result = append(result, address)
	}

	return result, nil
}

// Service identifiers for Concord peer discovery. The format follows RFC 6763.
//
// * MDNSService (_concord._udp) is used for mDNS / LAN discovery.
// Nodes on the same local network advertise themselves via multicast.
// Other nodes discover them by browsing for this service.
//
// * DNSSRVService (_concord._tcp) is used for DNS SRV record discovery.
// Operators add SRV records to a DNS zone so nodes can resolve candidate
// peer addresses through standard DNS queries.
const (
	MDNSService   = "_concord._udp"
	DNSSRVService = "_concord._tcp"
)
