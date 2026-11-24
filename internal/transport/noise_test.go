// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

package transport

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureStaticKeyGeneratesAndReloads(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	key, err := EnsureStaticKey()
	if err != nil {
		t.Fatalf("unexpected error generating noise key: %v", err)
	}

	if len(key.Private) != 32 || len(key.Public) != 32 {
		t.Fatalf("expected 32-byte key halves, got %d and %d", len(key.Private), len(key.Public))
	}

	// Calling again loads the same key from disk.
	second, err := EnsureStaticKey()
	if err != nil {
		t.Fatalf("unexpected error reloading noise key: %v", err)
	}

	if !bytes.Equal(second.Private, key.Private) ||
		!bytes.Equal(second.Public, key.Public) {
		t.Fatalf("expected reloaded key to match generated key")
	}

	// The file lives under concord/noise/secret.key with mode 0600.
	keyPath := filepath.Join(tmpDir, "concord", "noise", "secret.key")
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat noise key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 file mode, got %o", info.Mode().Perm())
	}
}

func TestEnsureStaticKeyRejectsWrongLength(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	keyPath := filepath.Join(tmpDir, "concord", "noise", "secret.key")

	err := os.MkdirAll(filepath.Dir(keyPath), 0o700)
	if err != nil {
		t.Fatalf("create noise dir: %v", err)
	}

	short := []byte("too-short")
	err = os.WriteFile(keyPath, short, 0o600)
	if err != nil {
		t.Fatalf("write short key: %v", err)
	}

	_, err = EnsureStaticKey()
	if err == nil {
		t.Fatalf("expected error for wrong-length key file")
	}

	// The bad file is left in place, never overwritten.
	// #nosec G304: path is inside the test temp dir.
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key file: %v", err)
	}
	if !bytes.Equal(raw, short) {
		t.Fatalf("expected bad key file to be preserved")
	}
}

func TestEnsureGenerationCounterInitializesAndReloads(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	gen, err := EnsureGenerationCounter()
	if err != nil {
		t.Fatalf("unexpected error initializing generation: %v", err)
	}
	if gen != 0 {
		t.Fatalf("initial generation = %d, want 0", gen)
	}

	// Reload returns the persisted value.
	gen, err = EnsureGenerationCounter()
	if err != nil {
		t.Fatalf("unexpected error reloading generation: %v", err)
	}
	if gen != 0 {
		t.Fatalf("reloaded generation = %d, want 0", gen)
	}

	// File holds one big-endian uint64 with mode 0600.
	genPath := filepath.Join(tmpDir, "concord", "noise", "generation")
	info, err := os.Stat(genPath)
	if err != nil {
		t.Fatalf("stat generation file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 file mode, got %o", info.Mode().Perm())
	}
}

func TestEnsureGenerationCounterRejectsWrongLength(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	genPath := filepath.Join(tmpDir, "concord", "noise", "generation")

	err := os.MkdirAll(filepath.Dir(genPath), 0o700)
	if err != nil {
		t.Fatalf("create noise dir: %v", err)
	}

	err = os.WriteFile(genPath, []byte("bad"), 0o600)
	if err != nil {
		t.Fatalf("write bad generation: %v", err)
	}

	_, err = EnsureGenerationCounter()
	if err == nil {
		t.Fatalf("expected error for wrong-length generation file")
	}
}

func TestRotateKeyBumpsGenerationAndDeletesKey(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	if _, err := EnsureStaticKey(); err != nil {
		t.Fatalf("ensure static key: %v", err)
	}

	gen, err := RotateKey(context.Background())
	if err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	if gen != 1 {
		t.Fatalf("generation = %d, want 1", gen)
	}

	// The key file is gone; the counter persisted.
	if _, err := os.Stat(filepath.Join(tmpDir, "concord", "noise", "secret.key")); !os.IsNotExist(err) {
		t.Fatalf("expected key file to be deleted, stat err = %v", err)
	}
	stored, err := EnsureGenerationCounter()
	if err != nil {
		t.Fatalf("read generation: %v", err)
	}
	if stored != 1 {
		t.Fatalf("stored generation = %d, want 1", stored)
	}

	// Rotating again bumps to 2 even with no key present.
	gen, err = RotateKey(context.Background())
	if err != nil {
		t.Fatalf("second rotate: %v", err)
	}
	if gen != 2 {
		t.Fatalf("generation = %d, want 2", gen)
	}
}

func TestRotateKeyRefusesExhaustedCounter(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	genPath := filepath.Join(tmpDir, "concord", "noise", "generation")

	err := os.MkdirAll(filepath.Dir(genPath), 0o700)
	if err != nil {
		t.Fatalf("create noise dir: %v", err)
	}

	full := bytes.Repeat([]byte{0xff}, 8)
	err = os.WriteFile(genPath, full, 0o600)
	if err != nil {
		t.Fatalf("write max generation: %v", err)
	}

	if _, err := RotateKey(context.Background()); err == nil {
		t.Fatal("rotation at max generation accepted")
	}
}
