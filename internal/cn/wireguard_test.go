// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestEnsureWGKeys(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	key, err := EnsureWGKeys()
	if err != nil {
		t.Fatalf("unexpected error generating wireguard keys: %v", err)
	}

	if key.Private == "" || key.Public == "" {
		t.Fatalf("expected non-empty private and public keys, got %+v", key)
	}

	privKey, err := wgtypes.ParseKey(key.Private)
	if err != nil {
		t.Fatalf("failed to parse private key: %v", err)
	}

	if privKey.PublicKey().String() != key.Public {
		t.Fatalf("public key mismatch: expected %s, got %s", privKey.PublicKey().String(), key.Public)
	}

	// Calling again should load the existing key from disk.
	secondKey, err := EnsureWGKeys()
	if err != nil {
		t.Fatalf("unexpected error re-loading wireguard keys: %v", err)
	}

	if secondKey.Private != key.Private || secondKey.Public != key.Public {
		t.Fatalf("expected cached keys to match: %+v vs %+v", key, secondKey)
	}

	// Verify file permissions are 0600.
	keyPath := filepath.Join(tmpDir, "concord", "wireguard", "wg.key")
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 file mode, got %o", info.Mode().Perm())
	}
}

func TestWGInterfaceName(t *testing.T) {
	nodeID := uuid.MustParse("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
	name := WGInterfaceName(nodeID)

	if !strings.HasPrefix(name, "wg-") {
		t.Fatalf("expected wg- prefix, got %s", name)
	}

	// Linux IFNAMSIZ is 16 bytes (including null terminator), so max length is 15.
	if len(name) > 15 {
		t.Fatalf("interface name %s exceeds IFNAMSIZ length limit of 15 chars", name)
	}

	if name != "wg-a1b2c3d4" {
		t.Fatalf("expected wg-a1b2c3d4, got %s", name)
	}
}

func TestSetNodeSubnetAndAllocateIP(t *testing.T) {
	SetNodeSubnet(3)
	ip1 := AllocateIP()
	if ip1 != "10.0.3.2/16" {
		t.Fatalf("expected 10.0.3.2/16, got %s", ip1)
	}

	ip2 := AllocateIP()
	if ip2 != "10.0.3.3/16" {
		t.Fatalf("expected 10.0.3.3/16, got %s", ip2)
	}

	SetNodeSubnet(0)
	ip0 := AllocateIP()
	if ip0 != "10.0.0.2/16" {
		t.Fatalf("expected 10.0.0.2/16, got %s", ip0)
	}
}
