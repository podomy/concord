// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"net/netip"
	"strings"
	"testing"
)

// Configs decode anchors and position; old configs without them stay valid
// with nil position and no anchors.
func TestDecodeNodeConfigAnchors(t *testing.T) {
	t.Parallel()

	cfg, err := decodeNodeConfig(strings.NewReader(`{
		"memberlist_address": "0.0.0.0:7946",
		"id": "3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77",
		"position": {"lat": 47.6, "lon": 8.9},
		"anchors": [
			{"name": "depot", "address": "192.168.100.10:7946", "lat": 47.61, "lon": 8.91},
			{"name": "mast", "address": "192.168.100.11:7946", "lat": 47.0, "lon": 8.0}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Position == nil || cfg.Position.Lat != 47.6 || cfg.Position.Lon != 8.9 {
		t.Fatalf("position = %+v", cfg.Position)
	}
	if len(cfg.Anchors) != 2 || cfg.Anchors[0].Name != "depot" {
		t.Fatalf("anchors = %+v", cfg.Anchors)
	}
	if p, ok := cfg.Anchors[0].AnchorPoint(); !ok || p.Lat != 47.61 {
		t.Fatalf("anchor point = %+v, %v", p, ok)
	}
}

// Old configs without position or anchors decode empty and non-anchor.
func TestDecodeNodeConfigBackwardCompatible(t *testing.T) {
	t.Parallel()

	old, err := decodeNodeConfig(strings.NewReader(`{
		"memberlist_address": "0.0.0.0:7946",
		"id": "3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if old.Position != nil || len(old.Anchors) != 0 {
		t.Fatalf("old config should decode empty: %+v", old)
	}
	if old.Anchor {
		t.Fatal("old config should not anchor")
	}
}

// The anchor role decodes from config separately: one role, one test.
func TestDecodeNodeConfigAnchor(t *testing.T) {
	t.Parallel()

	cfg, err := decodeNodeConfig(strings.NewReader(`{
		"memberlist_address": "0.0.0.0:7946",
		"id": "3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77",
		"anchor": true
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Anchor {
		t.Fatal("expected anchor flag")
	}
}

// Out-of-planet anchor coordinates never order anything.
func TestAnchorEntryInvalidPoint(t *testing.T) {
	t.Parallel()

	a := AnchorEntry{Addr: netip.MustParseAddrPort("192.168.100.10:7946"), Lat: 91, Lon: 0}
	if _, ok := a.AnchorPoint(); ok {
		t.Fatal("invalid coordinates should not order")
	}
}
