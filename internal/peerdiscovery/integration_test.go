// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/peerdiscovery"
)

// testGossipKeyValue is the fixed cluster-wide secret shared by all test
// nodes, mirroring one operator-provisioned key per test cluster.
var testGossipKeyValue = []byte("0123456789abcdef0123456789abcdef")

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "concord-test")
	if err != nil {
		panic(err)
	}
	keyDir := filepath.Join(dir, "concord", "memberservice")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		panic(err)
	}
	// #nosec G703 - test setup with trusted temp dir path.
	if err := os.WriteFile(filepath.Join(keyDir, "secret.key"), testGossipKeyValue, 0o600); err != nil {
		panic(err)
	}
	if err := os.Setenv("XDG_CONFIG_HOME", dir); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir) //nolint:errcheck // best-effort temp cleanup in test
	os.Exit(code)
}

func TestTwoNodesJoin(t *testing.T) {
	t.Parallel()

	svcA, addrA := startNode(t, nil)
	svcB, _ := startNode(t, []netip.AddrPort{addrA})

	waitForMembers(t, svcA, 2)
	waitForMembers(t, svcB, 2)
}

// Set values stay local until Publish pushes them: Members reads the last
// broadcast metadata, never the live atomics.
func TestPublishPropagates(t *testing.T) {
	t.Parallel()

	svc, _ := startNode(t, nil)

	svc.SetPressure(30, 40, 50)
	if got := selfMeta(t, svc); got.CPUPercent != 0 {
		t.Fatalf("unpublished pressure leaked: %+v", got)
	}

	if err := svc.Publish(); err != nil {
		t.Fatal(err)
	}
	got := selfMeta(t, svc)
	if got.CPUPercent != 30 || got.MemPercent != 40 || got.DiskPercent != 50 {
		t.Fatalf("pressure = %d/%d/%d", got.CPUPercent, got.MemPercent, got.DiskPercent)
	}

	var nilService *peerdiscovery.MemberService
	if err := nilService.Publish(); err != nil {
		t.Fatalf("nil publish: %v", err)
	}
}

// selfMeta returns the local member's metadata or fails.
func selfMeta(t *testing.T, svc *peerdiscovery.MemberService) peerdiscovery.NodeMetadata {
	t.Helper()

	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("members = %d, want 1", len(members))
	}
	return members[0].Metadata
}

func startNode(t *testing.T, join []netip.AddrPort) (*peerdiscovery.MemberService, netip.AddrPort) {
	t.Helper()

	svc, err := peerdiscovery.Start(zap.NewNop(), peerdiscovery.Node{
		ID:      uuid.New(),
		Address: netip.MustParseAddrPort("127.0.0.1:0"),
	}, join, netip.Addr{}, peerdiscovery.NoiseIdentity{})
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
