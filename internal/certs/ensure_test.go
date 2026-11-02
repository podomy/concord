// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/podomy/concord/internal/certs"
)

func TestEnsureFailsWithoutCA(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	if _, err := certs.Ensure(); err == nil {
		t.Fatal("expected error without CA")
	}
}

func TestEnsureSucceedsWithCA(t *testing.T) {
	setupCA(t)

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	assertRegularFile(t, paths.CA)
	assertRegularFile(t, paths.CAKey)

	wantDir := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "concord", "certs")
	if filepath.Dir(paths.CA) != wantDir {
		t.Fatalf("ca dir = %q, want %q", filepath.Dir(paths.CA), wantDir)
	}
}

// EnsureWithLogger returns the same paths as Ensure: a fresh CA is far
// from expiry, so no warning fires on this path.
func TestEnsureWithLoggerSucceedsWithCA(t *testing.T) {
	setupCA(t)

	plain, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	withLogger, err := certs.EnsureWithLogger(nil)
	if err != nil {
		t.Fatalf("ensure with logger: %v", err)
	}
	if plain != withLogger {
		t.Fatalf("paths = %+v, want %+v", withLogger, plain)
	}
}

func TestEnsureFailsWithMissingKey(t *testing.T) {
	setupCA(t)

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := os.Remove(paths.CAKey); err != nil {
		t.Fatalf("remove ca key: %v", err)
	}
	if _, err := certs.Ensure(); err == nil {
		t.Fatal("expected error with missing ca.key")
	}
}

func setupCA(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}
}

func assertRegularFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("%s is not a regular file", path)
	}
}
