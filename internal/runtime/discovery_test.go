// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"bytes"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/geo"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// Rewriting position in config shows up in gossip after one refresh,
// without a restart.
func TestRefreshPosition(t *testing.T) {
	svc := startPositionedService(t, 47.6, 8.9)

	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(members[0].Metadata.Lat-47.6) > 1e-6 || math.Abs(members[0].Metadata.Lon-8.9) > 1e-6 {
		t.Fatalf("position = %v/%v", members[0].Metadata.Lat, members[0].Metadata.Lon)
	}
}

// Moving means rewriting the file; the next refresh follows.
func TestRefreshPositionFollowsMoves(t *testing.T) {
	svc := startPositionedService(t, 47.6, 8.9)

	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		t.Fatal(err)
	}
	config.Position = &geo.Point{Lat: 48.0, Lon: 9.0}
	if _, err := node.UpdateNodeConfig(config); err != nil {
		t.Fatal(err)
	}
	refreshPosition(zap.NewNop(), svc)
	if err := svc.Publish(); err != nil {
		t.Fatal(err)
	}

	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(members[0].Metadata.Lat-48.0) > 1e-6 {
		t.Fatalf("moved lat = %v", members[0].Metadata.Lat)
	}
}

// refreshPosition is a test-only wrapper over refreshDiscoveryState: it
// reloads position from config and gossips it, discarding anchors.
// Production code calls refreshDiscoveryState directly so position and
// anchors refresh on the same read.
func refreshPosition(logger *zap.Logger, peerService *peerdiscovery.MemberService) *geo.Point {
	pos, _ := refreshDiscoveryState(logger, peerService, nil)
	return pos
}

// Removing position from config clears gossip to unknown instead of
// leaving the last fix converging forever.
func TestRefreshPositionRemovalClears(t *testing.T) {
	svc := startPositionedService(t, 47.6, 8.9)

	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		t.Fatal(err)
	}
	config.Position = nil
	if _, err := node.UpdateNodeConfig(config); err != nil {
		t.Fatal(err)
	}
	refreshPosition(zap.NewNop(), svc)
	if err := svc.Publish(); err != nil {
		t.Fatal(err)
	}

	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if members[0].Metadata.Lat != 0 || members[0].Metadata.Lon != 0 {
		t.Fatalf("cleared position = %v/%v, want 0/0", members[0].Metadata.Lat, members[0].Metadata.Lon)
	}
}

// startPositionedService provisions temp config with a gossip key,
// writes the given position, and starts one loopback member.
func startPositionedService(t *testing.T, lat, lon float64) *peerdiscovery.MemberService {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	keyDir := filepath.Join(dir, "concord", "memberservice")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "secret.key"), bytes.Repeat([]byte{0x07}, 32), 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		t.Fatal(err)
	}
	config.Position = &geo.Point{Lat: lat, Lon: lon}
	if _, err := node.UpdateNodeConfig(config); err != nil {
		t.Fatal(err)
	}

	svc, err := peerdiscovery.Start(zap.NewNop(), peerdiscovery.Node{
		ID:      uuid.New(),
		Address: netip.MustParseAddrPort("127.0.0.1:0"),
	}, nil, netip.Addr{}, peerdiscovery.NoiseIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := svc.Shutdown(); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	refreshPosition(zap.NewNop(), svc)
	if err := svc.Publish(); err != nil {
		t.Fatal(err)
	}
	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("members = %d, want 1", len(members))
	}
	return svc
}
