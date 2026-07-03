// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"fmt"
	"net/netip"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
)

// MemberService wraps memberlist membership tracking for Concord peer discovery.
type MemberService struct {
	list *memberlist.Memberlist
}

// Start creates a memberlist-backed peer discovery service for node.
//
// The node ID is used as memberlist's node name so discovered members can be
// mapped back to stable Concord identities. The node address is used as the
// local bind endpoint for memberlist traffic.
//
// If join is non-empty, Start attempts to join those bootstrap addresses before
// returning. If the join fails, the created memberlist is shut down before the
// error is returned.
func Start(node Node, join []netip.AddrPort) (*MemberService, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = node.ID.String()
	config.BindAddr = node.Address.Addr().String()
	config.BindPort = int(node.Address.Port())

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("memberlist create: %w", err)
	}

	if len(join) > 0 {
		if _, err := list.Join(addrPortsToStrings(join)); err != nil {
			if shutdownErr := list.Shutdown(); shutdownErr != nil {
				return nil, fmt.Errorf("list join: %w; shutdown: %w", err, shutdownErr)
			}

			return nil, fmt.Errorf("list join: %w", err)
		}
	}

	memberService := MemberService{
		list: list,
	}

	return &memberService, nil
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
