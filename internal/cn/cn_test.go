// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Integration test for the container networking layer. Requires root.
// Run with: sudo go test ./src/cn/ -v.
package cn

import (
	"context"
	"os"
	"testing"

	"github.com/vishvananda/netlink"
)

func TestBridgeCreate(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}

	ctx := context.Background()
	err := CreateBridge(ctx)
	if err != nil {
		t.Fatal(err)
	}

	link, err := netlink.LinkByName(bridgeName)
	if err != nil {
		t.Fatal(err)
	}
	if link == nil {
		t.Fatal("bridge not found")
	}
	if link.Attrs().Name != bridgeName {
		t.Fatalf("name = %s, want %s", link.Attrs().Name, bridgeName)
	}
}

func TestVethPairCreateAndDelete(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root")
	}

	ctx := context.Background()
	err := CreateBridge(ctx)
	if err != nil {
		t.Fatal(err)
	}

	containerID := "test-veth"
	cidr, err := CreateVethPair(ctx, containerID, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if cidr == "" {
		t.Fatal("no CIDR returned")
	}

	// Host end should exist and be attached to cn0.
	hostVethName := VethHostName(containerID, VethA)
	hostLink, err := netlink.LinkByName(hostVethName)
	if err != nil {
		t.Fatal(err)
	}
	if hostLink == nil {
		t.Fatal("host veth not found")
	}
	if hostLink.Attrs().MasterIndex == 0 {
		t.Fatal("host veth not attached to bridge")
	}

	// Delete it.
	err = DeleteLink(hostVethName)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's gone.
	_, err = netlink.LinkByName(hostVethName)
	if err == nil {
		t.Fatal("veth still exists after delete")
	}
}
