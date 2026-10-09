// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"fmt"
	"net"
	"net/netip"

	"github.com/google/uuid"
	"github.com/hashicorp/mdns"

	"github.com/podomy/concord/internal/node"
)

// NodeState describes memberlist's current liveness
// observation for a node.
type NodeState string

const (
	NodeStateAlive   NodeState = "alive"
	NodeStateSuspect NodeState = "suspect"
	NodeStateDead    NodeState = "dead"
	NodeStateLeft    NodeState = "left"
	NodeStateUnknown NodeState = "unknown"
)

// Node identifies a Concord node as it appears in peer
// discovery.
//
// ID is the stable Concord node identity. Address is the
// current network endpoint used by memberlist for peer
// membership traffic. The address can change over time; the
// ID is the durable identity. State is memberlist's
// current liveness observation for the node.
type Node struct {
	State    NodeState
	Address  netip.AddrPort
	Metadata NodeMetadata
	ID       uuid.UUID
}

// NodeMetadata carries dynamic metrics and networking
// capabilities gossiped across the cluster.
type NodeMetadata struct {
	WireGuardPublicKey string  `json:"wireguard_public_key"`
	CPUMHz             float64 `json:"cpu_mhz"`
	MemoryMB           uint64  `json:"memory_mb"`
	Workloads          int     `json:"workload_count"`
	// CPUPercent, MemPercent, and DiskPercent are 0-100 utilization sampled
	// on the fast beat. Small by design: gossip caps metadata at 512 bytes.
	CPUPercent  uint8 `json:"cpu_percent"`
	MemPercent  uint8 `json:"mem_percent"`
	DiskPercent uint8 `json:"disk_percent"`
	// Lat and Lon are this node's geographic position in decimal degrees.
	// Operator-provisioned and stationary in practice; members without a
	// known position gossip zeros, which never order anything.
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
	// Anchor marks a rendezvous anchor: a stable reachable member newcomers
	// may join. Provisioned with concord --anchor.
	Anchor bool `json:"anchor"`
	// NoisePublicKey is this node's Noise static public key (32 raw bytes,
	// base64 in JSON). Dialers use it as the IK handshake's pre-known peer
	// key, so no dial path needs a key the gossip layer did not provide.
	// The CA signature over the parcel travels inside the handshake, not
	// here: at 256 bytes it would blow memberlist's 512-byte metadata limit.
	NoisePublicKey []byte `json:"noise_public_key,omitempty"`
	// NoiseGeneration is the rotation counter of NoisePublicKey. Highest
	// generation seen for a node wins.
	NoiseGeneration uint64 `json:"noise_generation,omitempty"`
}

// equalVolatile reports whether the gossip-volatile subset matches: workload
// count, pressure trio, position, and anchor role. Static identity (keys,
// capacities) is excluded on purpose: it never changes at runtime, so
// comparing it would rebroadcast on every Publish. Add new volatile fields
// here, not at the call site, so Publish can never forget them.
func (m NodeMetadata) equalVolatile(o NodeMetadata) bool {
	return m.Workloads == o.Workloads &&
		m.CPUPercent == o.CPUPercent &&
		m.MemPercent == o.MemPercent &&
		m.DiskPercent == o.DiskPercent &&
		m.Lat == o.Lat &&
		m.Lon == o.Lon &&
		m.Anchor == o.Anchor
}

// NoiseIdentity is this node's Noise identity for gossip: the static public
// key, its rotation generation, and the CA signature binding all three to the
// node ID. The private half never leaves disk; see transport.EnsureStaticKey.
type NoiseIdentity struct {
	Pub        []byte
	Signature  []byte
	Generation uint64
}

// Resolver discovers candidate peer addresses for
// bootstrapping memberlist membership. Different
// implementations cover different discovery mechanisms such
// as LAN broadcast or a rendezvous anchor.
//
// Resolve must be safe for concurrent calls from multiple
// goroutines.
type Resolver interface {
	Resolve(ctx context.Context) ([]netip.AddrPort, error)
}

// ResolverFunc is an adapter that turns a plain function
// into a Resolver.
type ResolverFunc func(ctx context.Context) ([]netip.AddrPort, error)

// Resolve calls the underlying function.
func (f ResolverFunc) Resolve(
	ctx context.Context,
) ([]netip.AddrPort, error) {
	return f(ctx)
}

// MDNSAdvertise publishes this node on the local network
// under DNSService. It advertises only the resolved
// underlay address, the same one memberlist publishes, so
// peers never learn overlay endpoints such as cn0 or
// docker0. Advertising all interfaces leaks those
// addresses into peer candidate pools, and joining them
// dials the local bridge instead of a peer. A nil IP list
// falls back to all interfaces and is used only when no
// usable address resolves.
func MDNSAdvertise(
	ctx context.Context,
	nodeConfig *node.NodeConfig,
) (*mdns.Server, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf(
			"context cancellation: %w",
			err,
		)
	}

	resolved := ResolveAdvertise(
		nodeConfig.MemberlistAddress,
		nodeConfig.AdvertiseAddress,
	)
	var ips []net.IP
	if resolved.IsValid() {
		ips = []net.IP{net.IP(resolved.Addr().AsSlice())}
	}
	service, err := mdns.NewMDNSService(
		nodeConfig.ID.String(), // unique name - use node ID string.
		DNSService,
		"", // domain, empty = local.
		"", // hostname, empty = auto.
		int(nodeConfig.MemberlistAddress.Port()),
		ips, // nil when unresolvable: advertise all interfaces.
		nil, // TXT records optional.
	)
	if err != nil {
		return nil, fmt.Errorf(
			"mdns service creation: %w",
			err,
		)
	}

	server, err := mdns.NewServer(
		&mdns.Config{Zone: service},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"mdns server creation: %w",
			err,
		)
	}

	return server, nil
}

// Service identifiers for Concord peer discovery. The
// format follows RFC 6763.
//
// * MDNSService (_concord._udp) is used for mDNS / LAN
// discovery. Nodes on the same local network advertise
// themselves via multicast.
// Other nodes discover them by browsing for this service.
//
// * DNSService (_concord._udp) names the embedded DNS server's service.
// Workloads resolve peer names against it; member discovery no longer
// queries it, gossip carries membership instead.
var (
	DNSService = "_concord._udp"
	DNSPort    = "8053"
)
