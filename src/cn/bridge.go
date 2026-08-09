// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"syscall"

	"github.com/vishvananda/netlink"
)

const (
	bridgeName = "cn0"
)

var (
	nextContainerIP = netip.MustParseAddr("10.0.0.2")
	ipMu            sync.Mutex
)

// Invariants
// 1. One bridge per node. cn0 exists as long as concord is running.
// It has to be idempotent (create-if-not exists).
// 2. One veth pair per container. Created on container start, destroyed on stopping.
// Bridge attachment though needs a manual cleanup.
// 3. IP assignment is deterministic per workload. A container gets the same.
// 10.0.0.x IP every time from the bridge's subnet. Rely on the veth's host-side
// MAC as a persistent identifier since workloads persist by identity.
// 4. Port mapping is a one shot add/remove. A DNAT rule in iptables for each
// HOSTPORT -> CONTAINERIP:CONTAINERPORT. Removed atomically on container stop.

// CreateBridge ensures the cn0 bridge exists, is up, and has the subnet
// address 10.0.0.1/16 assigned. It is idempotent, safe to call on every
// startup.
func CreateBridge(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	// First usable address of the /16 subnet.
	ip := "10.0.0.1"
	mask := "/16"

	// Create the bridge.
	bridge := &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{
			Name: bridgeName,
		},
	}

	// Idempotently create the link device for the bridge.
	linkDevice, err := findOrCreateBridgeLinkDevice(ctx, bridge)
	if err != nil {
		return fmt.Errorf("find or create link device: %w", err)
	}

	// Enable the link device.
	err = netlink.LinkSetUp(linkDevice)
	if err != nil {
		return fmt.Errorf("netlink link set up: %w", err)
	}

	// Add an address to the link device.
	linkAddress, err := netlink.ParseAddr(ip + mask)
	if err != nil {
		return fmt.Errorf("netlink parseaddr: %w", err)
	}

	err = netlink.AddrAdd(linkDevice, linkAddress)
	if err != nil && !errors.Is(err, syscall.EEXIST) {
		return fmt.Errorf("netlink addr add: %w", err)
	}

	// NAT setup. Setup of the masquerade to rewrite source ip
	// of the outgoing container packets to the ip of the host.
	err = SetupMasquerade(ctx)
	if err != nil {
		return fmt.Errorf("setup masquerade: %w", err)
	}

	return nil
}

// findOrCreateBridgeLinkDevice returns the existing cn0 link device or
// creates it via netlink if it does not already exist.
func findOrCreateBridgeLinkDevice(ctx context.Context, bridge *netlink.Bridge) (netlink.Link, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	// Search for the interface first, if it exists do not add it.
	linkDevice, errLinkByName := netlink.LinkByName(bridge.Name)
	if errLinkByName != nil {
		// Add the interface to the system.
		errLinkAdd := netlink.LinkAdd(bridge)
		if errLinkAdd != nil {
			return nil, fmt.Errorf("netlink link add: %w", errLinkAdd)
		}
		linkDevice = bridge
	}

	return linkDevice, nil
}

// AllocateIP allocates an available IP address and returns that as a string of IP + CIDR.
func AllocateIP() string {
	ipMu.Lock()
	ip := nextContainerIP
	nextContainerIP = nextContainerIP.Next()
	ipMu.Unlock()
	return ip.String() + "/16"
}
