// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery_test

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/dnsserver"
	"github.com/podomy/concord/src/peerdiscovery"
)

func TestTwoNodesJoin(t *testing.T) {
	t.Parallel()

	svcA, addrA := startNode(t, nil)
	svcB, _ := startNode(t, []netip.AddrPort{addrA})

	waitForMembers(t, svcA, 2)
	waitForMembers(t, svcB, 2)
}

func TestTwoNodesDNSDiscovery(t *testing.T) {
	t.Parallel()

	svcA, addrA := startNode(t, nil)
	dnsPort := freeUDPPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := dnsserver.Start(ctx, svcA, zap.NewNop(), "127.0.0.1:"+dnsPort); err != nil {
		t.Fatalf("start dns: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	got, err := (&peerdiscovery.DNSSRVResolver{
		Bootstrap: []netip.AddrPort{addrA},
		Timeout:   time.Second,
		QueryPort: dnsPort,
	}).Resolve(context.Background())
	if err != nil {
		t.Fatalf("dns srv resolve: %v", err)
	}
	if !containsPort(got, addrA.Port()) {
		t.Fatalf("discovered %v, expected port of A %v", got, addrA)
	}
}

func startNode(t *testing.T, join []netip.AddrPort) (*peerdiscovery.MemberService, netip.AddrPort) {
	t.Helper()

	svc, err := peerdiscovery.Start(zap.NewNop(), peerdiscovery.Node{
		ID:      uuid.New(),
		Address: netip.MustParseAddrPort("127.0.0.1:0"),
	}, join)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	addr, err := svc.LocalAddr()
	if err != nil {
		t.Fatalf("local addr: %v", err)
	}
	if addr.Port() == 0 {
		t.Fatal("expected non-zero bound port")
	}
	return svc, addr
}

func waitForMembers(t *testing.T, svc *peerdiscovery.MemberService, minCount int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for {
		members, err := svc.Members()
		if err != nil {
			t.Fatalf("members: %v", err)
		}
		if len(members) >= minCount {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("membership not converged: have %d, want >= %d", len(members), minCount)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func freeUDPPort(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	pc, err := lc.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		if err := pc.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	udpAddr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("unexpected addr type %T", pc.LocalAddr())
	}
	return strconv.Itoa(udpAddr.Port)
}

func containsPort(addrs []netip.AddrPort, port uint16) bool {
	for _, addr := range addrs {
		if addr.Port() == port {
			return true
		}
	}
	return false
}
