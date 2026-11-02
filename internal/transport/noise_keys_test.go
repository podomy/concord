// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureStaticKeyRejectsWorldReadable(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	_, err := EnsureStaticKey()
	if err != nil {
		t.Fatalf("generate noise key: %v", err)
	}

	keyPath := filepath.Join(tmpDir, "concord", "noise", "secret.key")
	// #nosec G304: path is inside the test temp dir.
	before, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}

	err = os.Chmod(keyPath, 0o644) // #nosec G302: deliberately permissive to test fail-closed key handling.
	if err != nil {
		t.Fatalf("chmod key file: %v", err)
	}

	_, err = EnsureStaticKey()
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

func TestEnsureGenerationCounterRejectsWorldReadable(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	_, err := EnsureGenerationCounter()
	if err != nil {
		t.Fatalf("init generation: %v", err)
	}

	genPath := filepath.Join(tmpDir, "concord", "noise", "generation")
	err = os.Chmod(genPath, 0o644) // #nosec G302: deliberately permissive to test fail-closed key handling.
	if err != nil {
		t.Fatalf("chmod generation file: %v", err)
	}

	_, err = EnsureGenerationCounter()
	if err == nil {
		t.Fatalf("expected error for world-readable generation file")
	}
	if !strings.Contains(err.Error(), "0600") {
		t.Fatalf("expected permission error, got: %v", err)
	}
}
