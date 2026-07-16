// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs_test

import (
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/certs"
)

func TestEnsureMintsAndReuses(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	id := uuid.New()
	paths, err := certs.Ensure(id)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	assertRegularFile(t, paths.CA)
	assertRegularFile(t, paths.Cert)
	assertRegularFile(t, paths.Key)

	again, err := certs.Ensure(id)
	if err != nil {
		t.Fatalf("ensure reuse: %v", err)
	}
	if again.CA != paths.CA {
		t.Fatalf("ca path = %q, want %q", again.CA, paths.CA)
	}
}

func TestEnsureRemintsInvalid(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	id := uuid.New()
	paths, err := certs.Ensure(id)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := os.WriteFile(paths.Cert, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatalf("corrupt cert: %v", err)
	}
	if _, err := certs.Ensure(id); err != nil {
		t.Fatalf("ensure after corrupt: %v", err)
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
