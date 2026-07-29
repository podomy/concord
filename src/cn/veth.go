// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"fmt"

	"github.com/vishvananda/netlink"
)

// Invariants
// 1. One veth pair per container. Created on container start, cleaned up on stop.
// 2. The host end attached to cn0. The container end moves into the workload's
// network namespace.
// 3. Veth names are deterministic. veth-<workload-id>a and veth-<workload-id>b.
// a is the host side, b is the container side.
// 4. IP assignment happens outside the container's namespace before veth is moved.
// 5. Cleanup removes both ends. The kernel deletes the container end when its
// namespace dies, but the host end must be explicitly removed.

// CreateVethPair creates a veth pair for a workload, moves the container
// end into the workload's network namespace, attaches the host end to cn0,
// and brings the host end up.
func CreateVethPair(ctx context.Context, containerID string, namespacePID int) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	vethA := VethHostName(containerID, VethA)
	vethB := VethHostName(containerID, VethB)

	vethALink, err := setupVethLink(ctx, vethA, vethB, namespacePID)
	if err != nil {
		return fmt.Errorf("setup veth link: %w", err)
	}

	// Get the bridge.
	bridgeLink, err := netlink.LinkByName(bridgeName)
	if err != nil {
		return fmt.Errorf("link by name: %w", err)
	}

	// Attach vethA to the bridge.
	err = netlink.LinkSetMaster(vethALink, bridgeLink)
	if err != nil {
		return fmt.Errorf("link set master: %w", err)
	}

	// Bring the vethA link up.
	err = netlink.LinkSetUp(vethALink)
	if err != nil {
		return fmt.Errorf("link set up: %w", err)
	}

	return nil
}

// findOrCreateVethLinkDevice returns the existing veth link or creates it
// via netlink if it does not already exist.
func findOrCreateVethLinkDevice(ctx context.Context, veth *netlink.Veth) (netlink.Link, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	// Search for the interface first, if it exists do not add it.
	linkDevice, errLinkByName := netlink.LinkByName(veth.Name)
	if errLinkByName != nil {
		// Add the interface to the system.
		errLinkAdd := netlink.LinkAdd(veth)
		if errLinkAdd != nil {
			return nil, fmt.Errorf("link add: %w", errLinkAdd)
		}
		linkDevice = veth
	}

	return linkDevice, nil
}

// DeleteLink removes the host end of a veth pair from the system. The
// container end is cleaned up by the kernel when the network namespace dies.
func DeleteLink(linkName string) error {
	linkDevice, err := netlink.LinkByName(linkName)
	if err != nil {
		return fmt.Errorf("link by name: %w", err)
	}

	err = netlink.LinkDel(linkDevice)
	if err != nil {
		return fmt.Errorf("link del: %w", err)
	}

	return nil
}

// setupVethLink create the veth pairs. It returns end A, and an error.
// The end A goes to the host, the end B goes to the other namespace (i.e. container).
func setupVethLink(ctx context.Context, vethA, vethB string, namespacePID int) (netlink.Link, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	link := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name: vethA, // host side
		},
		PeerName: vethB, // container side
	}

	vethALink, err := findOrCreateVethLinkDevice(ctx, link)
	if err != nil {
		return nil, fmt.Errorf("find or create veth link device: %w", err)
	}

	// Changing the namespace of the b end to the namespace of the container
	// The link becomes unavailable after this change on host.
	vethBLink, err := netlink.LinkByName(vethB)
	if err != nil {
		return nil, fmt.Errorf("link by name: %w", err)
	}

	// Set the address for the link device / interface before putting it into the
	// namespace of the container.
	addr, err := netlink.ParseAddr(AllocateIP())
	if err != nil {
		return nil, fmt.Errorf("netlink parse addr: %w", err)
	}
	err = netlink.AddrAdd(vethBLink, addr)
	if err != nil {
		return nil, fmt.Errorf("netlink addr add: %w", err)
	}

	err = netlink.LinkSetUp(vethBLink)
	if err != nil {
		return nil, fmt.Errorf("link set up B: %w", err)
	}

	err = netlink.LinkSetNsPid(vethBLink, namespacePID)
	if err != nil {
		return nil, fmt.Errorf("link set ns pid: %w", err)
	}

	return vethALink, nil
}

type VethEnd string

const (
	VethA VethEnd = "a"
	VethB VethEnd = "b"
)

func VethHostName(containerID string, whichEnd VethEnd) string {
	return "veth-<" + containerID + ">" + string(whichEnd)
}
