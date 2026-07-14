// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// MemberService wraps memberlist membership tracking for Concord peer discovery.
type MemberService struct {
	list *memberlist.Memberlist
}

// Start creates a memberlist-backed peer discovery service for node.
//
// The node ID is used as memberlist's node name so discovered members can be
// mapped back to stable Concord identities. The node address is the local bind
// endpoint for memberlist traffic. Port 0 asks the OS for an ephemeral port;
// use LocalAddr after Start to learn the bound endpoint.
//
// advertise is an optional IP hint from config overriding which IP to
// publish (see ResolveAdvertise). A zero Addr means auto-detect.
//
// If join is non-empty, Start attempts to join those bootstrap addresses.
// A failed join does not abort startup: the node keeps running as a solo
// member (membership = this node only). Discovery may have returned a
// non-Concord host; soft-fail avoids taking the whole process down. Callers
// can retry Join later.
func Start(logger *zap.Logger, node Node, join []netip.AddrPort, advertise netip.Addr) (*MemberService, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = node.ID.String()
	config.BindAddr = node.Address.Addr().String()
	config.BindPort = int(node.Address.Port())

	resolved := ResolveAdvertise(node.Address, advertise)
	if resolved.IsValid() {
		config.AdvertiseAddr = resolved.Addr().String()
		if resolved.Port() != 0 {
			config.AdvertisePort = int(resolved.Port())
		}
	}
	// Route memberlist's stdlib logger through zap (JSON with the rest of the app).
	if logger != nil {
		config.Logger = zap.NewStdLog(logger.Named("memberlist"))
	}

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("memberlist bind %s: %w", node.Address, err)
	}

	// Soft-fail join: bad discovery candidates must not kill Concord.
	// The local memberlist stays up; membership is just this node until a
	// later successful Join or inbound gossip.
	if len(join) > 0 {
		if _, err := list.Join(addrPortsToStrings(join)); err != nil {
			if logger != nil {
				logger.Warn("memberlist join failed; continuing alone",
					zap.Error(err),
					zap.Int("candidates", len(join)),
				)
			}
		}
	}

	memberService := MemberService{
		list: list,
	}

	return &memberService, nil
}

// ResolveAdvertise picks the address other peers should dial when contacting
// this node. Callers pass the memberlist bind address and an optional config
// override.
//
// advertise is the operator-supplied IP hint (AdvertiseAddress from config).
// Empty (zero Addr) means the runtime picks one from the bind address or a
// non-loopback interface. The port always comes from bind regardless.
//
// Resolution order:
//  1. Advertise hint if set (IP from config, port from bind).
//  2. The bind address if it is already a real IP peers can dial.
//  3. Otherwise the first usable host interface IP, with the bind port.
//
// Returns the zero AddrPort only if no usable address exists; memberlist will
// then choose an interface on its own.
func ResolveAdvertise(bind netip.AddrPort, advertise netip.Addr) netip.AddrPort {
	// Operator override: which IP to publish. Port always matches bind so
	// peers dial the same port memberlist is listening on.
	if advertise.IsValid() {
		return netip.AddrPortFrom(advertise, bind.Port())
	}

	// Bind is already a specific IP (not 0.0.0.0 / ::). Peers can dial that
	// same address, so advertise it as-is. IsUnspecified() is true only for
	// the wildcard addresses that mean "listen on every interface".
	if bind.Addr().IsValid() && !bind.Addr().IsUnspecified() {
		return bind
	}

	// Bind is a wildcard (listen everywhere). Peers cannot dial 0.0.0.0, so
	// take the first non-loopback interface IP and attach the bind port.
	return firstRoutableAddrPort(bind.Port())
}

// firstRoutableAddrPort returns the first non-loopback host address with the
// given port. Used when bind is 0.0.0.0 so we still publish something peers
// can reach. Order follows net.InterfaceAddrs (OS-dependent).
func firstRoutableAddrPort(port uint16) netip.AddrPort {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		// No interfaces visible; caller gets zero and memberlist may guess.
		return netip.AddrPort{}
	}
	for _, ia := range addrs {
		addr, ok := addrFromInterface(ia)
		if !ok {
			// Skip loopback, link-local, non-IP, or unusable entries.
			continue
		}
		// First usable address wins; port comes from memberlist bind.
		return netip.AddrPortFrom(addr, port)
	}
	// Host only has loopback / link-local (or empty); nothing to advertise.
	return netip.AddrPort{}
}

// addrFromInterface converts a net.Addr from net.InterfaceAddrs into a
// dialable netip.Addr. Returns false for anything peers should not use:
// non-IP nets, nil IPs, loopback, link-local, or unspecified addresses.
func addrFromInterface(ia net.Addr) (netip.Addr, bool) {
	// InterfaceAddrs yields *net.IPNet for IP configurations.
	ipNet, ok := ia.(*net.IPNet)
	if !ok || ipNet.IP == nil {
		return netip.Addr{}, false
	}

	ip := ipNet.IP
	// Filter before conversion: loopback (127.0.0.1), link-local (169.254.x /
	// fe80::), and unspecified (0.0.0.0) are not useful cluster endpoints.
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return netip.Addr{}, false
	}

	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}

	// Unmap turns IPv4-mapped IPv6 (::ffff:a.b.c.d) into plain IPv4 so peers
	// see a normal address form.
	addr = addr.Unmap()
	// Re-check after Unmap in case mapping produced a still-unusable address.
	if !addr.IsValid() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return netip.Addr{}, false
	}
	return addr, true
}

// LocalAddr returns the address memberlist is advertising for this node.
func (m *MemberService) LocalAddr() (netip.AddrPort, error) {
	n := m.list.LocalNode()
	addr, ok := netip.AddrFromSlice(n.Addr)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("invalid local member address: %v", n.Addr)
	}
	return netip.AddrPortFrom(addr.Unmap(), n.Port), nil
}

// Join asks the running memberlist service to join existing peer addresses.
//
// The addresses are bootstrap endpoints, not durable peer identities. After a
// successful join, memberlist exchanges membership state and Members can return
// the reachable nodes known to the local process.
func (m *MemberService) Join(existingMembers []netip.AddrPort) (int, error) {
	n, err := m.list.Join(addrPortsToStrings(existingMembers))
	if err != nil {
		return 0, fmt.Errorf("list join: %w", err)
	}

	return n, nil
}

// Members returns the current memberlist view as Concord peer discovery nodes.
//
// Memberlist stores each member's identity in Node.Name. Concord writes UUIDs
// into that field when starting memberlist, so this method parses the name back
// into a uuid.UUID and pairs it with the member's advertised address.
func (m *MemberService) Members() ([]Node, error) {
	members := m.list.Members()

	out := make([]Node, 0, len(members))

	for _, member := range members {
		id, err := uuid.Parse(member.Name)
		if err != nil {
			return nil, fmt.Errorf("parse member id: %w", err)
		}

		addr, ok := netip.AddrFromSlice(member.Addr)
		if !ok {
			return nil, fmt.Errorf("invalid member address: %v", member.Addr)
		}

		out = append(out, Node{
			ID:      id,
			Address: netip.AddrPortFrom(addr, member.Port),
			State:   memberState(member.State),
		})
	}

	return out, nil
}

// Shutdown stops the memberlist service and releases its network resources.
func (m *MemberService) Shutdown() error {
	err := m.list.Shutdown()
	if err != nil {
		return fmt.Errorf("memberservice shutdown: %w", err)
	}

	return nil
}

// addrPortsToStrings converts strongly typed endpoints into the string format
// expected by memberlist.Join.
func addrPortsToStrings(addresses []netip.AddrPort) []string {
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, address.String())
	}

	return out
}

// memberState maps memberlist's liveness states into Concord peer discovery states.
func memberState(state memberlist.NodeStateType) NodeState {
	switch state {
	case memberlist.StateAlive:
		return NodeStateAlive
	case memberlist.StateSuspect:
		return NodeStateSuspect
	case memberlist.StateDead:
		return NodeStateDead
	case memberlist.StateLeft:
		return NodeStateLeft
	default:
		return NodeStateUnknown
	}
}
