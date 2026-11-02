// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureWGKeysRejectsCorrupt(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	keyPath := filepath.Join(tmpDir, "concord", "wireguard", "wg.key")
	err := os.MkdirAll(filepath.Dir(keyPath), 0o700)
	if err != nil {
		t.Fatalf("create wireguard dir: %v", err)
	}

	bad := "not-a-valid-wireguard-key\n"
	err = os.WriteFile(keyPath, []byte(bad), 0o600)
	if err != nil {
		t.Fatalf("write corrupt key: %v", err)
	}

	_, err = EnsureWGKeys()
	if err == nil {
		t.Fatalf("expected error for corrupt key file")
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("expected corruption error, got: %v", err)
	}

	// #nosec G304: path is inside the test temp dir.
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if string(raw) != bad {
		t.Fatalf("expected corrupt key file to be preserved, got %q", string(raw))
	}
}

func TestEnsureWGKeysRejectsWorldReadable(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	_, err := EnsureWGKeys()
	if err != nil {
		t.Fatalf("generate wireguard keys: %v", err)
	}

	keyPath := filepath.Join(tmpDir, "concord", "wireguard", "wg.key")
	// #nosec G304: path is inside the test temp dir.
	before, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}

	err = os.Chmod(keyPath, 0o644) // #nosec G302: deliberately permissive to test fail-closed key handling.
	if err != nil {
		t.Fatalf("chmod key file: %v", err)
	}

	_, err = EnsureWGKeys()
	if err == nil {
		t.Fatalf("expected error for world-readable key file")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Fatalf("expected permission error, got: %v", err)
	}

	// #nosec G304: path is inside the test temp dir.
	after, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("expected key file to be preserved on permission refusal")
	}
}
