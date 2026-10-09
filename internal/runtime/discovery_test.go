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

// Boot publishes the persisted config position without any live fix:
// the file seeds the first gossip, IPC moves it after.
func TestBootPositionPublished(t *testing.T) {
	svc, sampler := startPositionedService(t, 47.6, 8.9)

	members, err := svc.Members()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(members[0].Metadata.Lat-47.6) > 1e-6 || math.Abs(members[0].Metadata.Lon-8.9) > 1e-6 {
		t.Fatalf("position = %v/%v", members[0].Metadata.Lat, members[0].Metadata.Lon)
	}
	if trail := sampler.Trail(); len(trail) != 1 || trail[0].Pos.Lat != 47.6 {
		t.Fatalf("boot trail = %+v", trail)
	}
}

// Rewriting anchors in config shows up on the next refresh: the
// discovery round never uses a boot snapshot past its fallback.
func TestRefreshAnchorsFollowsRewrites(t *testing.T) {
	startPositionedService(t, 47.6, 8.9)

	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		t.Fatal(err)
	}
	config.Anchors = []node.AnchorEntry{
		{Name: "rim", Addr: netip.MustParseAddrPort("192.168.100.11:7946"), Lat: 47.0, Lon: 8.0},
	}
	if _, err := node.UpdateNodeConfig(config); err != nil {
		t.Fatal(err)
	}

	live := refreshAnchors(zap.NewNop(), nil)
	if len(live) != 1 || live[0].Name != "rim" {
		t.Fatalf("anchors = %+v", live)
	}
}

// startPositionedService provisions temp config with a gossip key,
// writes the given position, and starts one loopback member with the
// boot position published and the trail seeded.
func startPositionedService(t *testing.T, lat, lon float64) (*peerdiscovery.MemberService, *node.Sampler) {
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
	nodeConfig, err := node.UpdateNodeConfig(config)
	if err != nil {
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

	sampler := node.NewSampler()
	publishPosition(nodeConfig, svc, sampler)
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
	return svc, sampler
}
